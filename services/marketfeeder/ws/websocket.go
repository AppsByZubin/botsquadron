package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type authorizeResponse struct {
	Status string `json:"status"`
	Data   struct {
		AuthorizedRedirectURI string `json:"authorized_redirect_uri"`
	} `json:"data"`
}

type subscriptionRequest struct {
	GUID   string `json:"guid"`
	Method string `json:"method"`
	Data   struct {
		Mode           string   `json:"mode,omitempty"`
		InstrumentKeys []string `json:"instrumentKeys"`
	} `json:"data"`
}

func main() {
	token := strings.TrimSpace(os.Getenv("UPSTOX_API_ACCESS_TOKEN"))
	if token == "" {
		log.Fatal("UPSTOX_API_ACCESS_TOKEN environment variable is required")
	}

	instrument := getenvDefault("UPSTOX_INSTRUMENT_KEY", "NSE_INDEX|Nifty 50")
	mode := getenvDefault("UPSTOX_MODE", "ltpc") // ltpc | full | option_greeks | full_d30

	authorizedWSURL, err := getAuthorizedWSURL(token)
	if err != nil {
		log.Fatalf("failed to get authorized websocket URL: %v", err)
	}

	log.Printf("authorized websocket URL received")

	headers := http.Header{}
	headers.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	headers.Set("Accept", "*/*")

	dialer := websocket.Dialer{
		HandshakeTimeout: 20 * time.Second,
	}

	conn, resp, err := dialer.Dial(authorizedWSURL, headers)
	if err != nil {
		log.Printf("websocket dial failed: %v", err)
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("response status: %s", resp.Status)
			log.Printf("response body: %s", string(body))
		}
		os.Exit(1)
	}
	defer conn.Close()

	log.Printf("connected to websocket")

	if err := sendSubscribe(conn, instrument, mode); err != nil {
		log.Fatalf("failed to send subscription: %v", err)
	}
	log.Printf("subscription sent for instrument=%s mode=%s", instrument, mode)

	// Upstox sends ping frames automatically when there is no market data.
	// Gorilla handles pong responses automatically, but we still keep a read deadline fresh.
	conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPongHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		log.Printf("received pong")
		return nil
	})
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		log.Printf("received ping")
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	readErrCh := make(chan error, 1)
	go func() {
		for {
			msgType, message, err := conn.ReadMessage()
			if err != nil {
				readErrCh <- err
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))

			switch msgType {
			case websocket.TextMessage:
				log.Printf("text frame: %s", string(message))
			case websocket.BinaryMessage:
				// Upstox Market Data Feed V3 returns protobuf/binary frames.
				// Without generated proto bindings we cannot decode them here,
				// so log a safe preview to confirm the stream is working.
				preview := message
				if len(preview) > 64 {
					preview = preview[:64]
				}
				log.Printf("binary frame received: %d bytes, first %d bytes(hex)=%s",
					len(message), len(preview), hex.EncodeToString(preview))
			default:
				log.Printf("frame received: type=%d bytes=%d", msgType, len(message))
			}
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("shutdown requested")
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"))
	case err := <-readErrCh:
		log.Printf("read loop ended: %v", err)
	}
}

func getAuthorizedWSURL(token string) (string, error) {
	const authorizeURL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

	req, err := http.NewRequest(http.MethodGet, authorizeURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("authorize API returned %s: %s", resp.Status, string(body))
	}

	var out authorizeResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("failed to parse authorize response: %w; body=%s", err, string(body))
	}
	if out.Data.AuthorizedRedirectURI == "" {
		return "", fmt.Errorf("authorize response missing authorized_redirect_uri: %s", string(body))
	}

	return out.Data.AuthorizedRedirectURI, nil
}

func sendSubscribe(conn *websocket.Conn, instrument, mode string) error {
	req := subscriptionRequest{
		GUID:   fmt.Sprintf("go-%d", time.Now().UnixNano()),
		Method: "sub",
	}
	req.Data.Mode = mode
	req.Data.InstrumentKeys = []string{instrument}

	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}

	// Upstox V3 docs specify that the request should be sent as a binary frame.
	return conn.WriteMessage(websocket.BinaryMessage, payload)
}

func getenvDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}
