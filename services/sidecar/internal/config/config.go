package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
)

type Config struct {
	HTTPAddr                    string
	WeightsPath                 string
	OutputPath                  string
	Timezone                    *time.Location
	TimezoneName                string
	IndexInstrumentKey          string
	BotID                       string
	MarketOpen                  string
	MarketClose                 string
	FinalizeGrace               time.Duration
	MaximumSnapshots            int
	NATSURL                     string
	NATSTickSubject             string
	NATSInstrumentSubject       string
	NATSReconnectWait           time.Duration
	NATSConnectTimeout          time.Duration
	SubscriptionRefreshInterval time.Duration
	HTTPReadHeaderTimeout       time.Duration
	HTTPReadTimeout             time.Duration
	HTTPWriteTimeout            time.Duration
	HTTPIdleTimeout             time.Duration
	ShutdownTimeout             time.Duration
}

func Load() (Config, error) {
	config := Config{
		HTTPAddr:              getenv("SIDECAR_HTTP_ADDR", ":8082"),
		WeightsPath:           getenv("SIDECAR_WEIGHTS_PATH", "files/official_nifty50_weights.csv"),
		OutputPath:            getenv("SIDECAR_OUTPUT_PATH", "files/dragger_puller.json"),
		TimezoneName:          getenv("APP_TIMEZONE", "Asia/Kolkata"),
		IndexInstrumentKey:    getenv("SIDECAR_INDEX_INSTRUMENT_KEY", model.Nifty50IndexKey),
		BotID:                 getenv("SIDECAR_BOT_ID", "sidecar"),
		MarketOpen:            getenv("SIDECAR_MARKET_OPEN", "09:15"),
		MarketClose:           getenv("SIDECAR_MARKET_CLOSE", "15:30"),
		NATSURL:               getenv("NATS_URL", "nats://localhost:4222"),
		NATSTickSubject:       getenv("SIDECAR_NATS_TICK_SUBJECT", "marketfeeder.tick_data"),
		NATSInstrumentSubject: getenv("SIDECAR_NATS_INSTRUMENT_SUBJECT", "marketfeeder.instrument_keys"),
	}
	var err error
	if config.Timezone, err = time.LoadLocation(config.TimezoneName); err != nil {
		return Config{}, fmt.Errorf("load APP_TIMEZONE %q: %w", config.TimezoneName, err)
	}
	if config.FinalizeGrace, err = duration("SIDECAR_FINALIZE_GRACE", 2*time.Second); err != nil {
		return Config{}, err
	}
	if config.SubscriptionRefreshInterval, err = duration("SIDECAR_SUBSCRIPTION_REFRESH_INTERVAL", 30*time.Second); err != nil {
		return Config{}, err
	}
	if config.NATSReconnectWait, err = secondsOrDuration("NATS_CONNECT_RETRY_WAIT_SEC", 2*time.Second); err != nil {
		return Config{}, err
	}
	if config.NATSConnectTimeout, err = secondsOrDuration("NATS_CONNECT_TIMEOUT_SEC", 5*time.Second); err != nil {
		return Config{}, err
	}
	if config.HTTPReadHeaderTimeout, err = duration("SIDECAR_HTTP_READ_HEADER_TIMEOUT", 5*time.Second); err != nil {
		return Config{}, err
	}
	if config.HTTPReadTimeout, err = duration("SIDECAR_HTTP_READ_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if config.HTTPWriteTimeout, err = duration("SIDECAR_HTTP_WRITE_TIMEOUT", 15*time.Second); err != nil {
		return Config{}, err
	}
	if config.HTTPIdleTimeout, err = duration("SIDECAR_HTTP_IDLE_TIMEOUT", 60*time.Second); err != nil {
		return Config{}, err
	}
	if config.ShutdownTimeout, err = duration("SIDECAR_SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if config.MaximumSnapshots, err = positiveInt("SIDECAR_MAX_SNAPSHOTS", 600); err != nil {
		return Config{}, err
	}
	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (config Config) validate() error {
	required := map[string]string{
		"SIDECAR_HTTP_ADDR":               config.HTTPAddr,
		"SIDECAR_WEIGHTS_PATH":            config.WeightsPath,
		"SIDECAR_OUTPUT_PATH":             config.OutputPath,
		"SIDECAR_INDEX_INSTRUMENT_KEY":    config.IndexInstrumentKey,
		"SIDECAR_BOT_ID":                  config.BotID,
		"NATS_URL":                        config.NATSURL,
		"SIDECAR_NATS_TICK_SUBJECT":       config.NATSTickSubject,
		"SIDECAR_NATS_INSTRUMENT_SUBJECT": config.NATSInstrumentSubject,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not be empty", name)
		}
	}
	if _, _, err := net.SplitHostPort(config.HTTPAddr); err != nil {
		return fmt.Errorf("SIDECAR_HTTP_ADDR %q must be host:port: %w", config.HTTPAddr, err)
	}
	if config.FinalizeGrace < 0 || config.FinalizeGrace >= time.Minute {
		return fmt.Errorf("SIDECAR_FINALIZE_GRACE must be between 0 and 1 minute")
	}
	if config.SubscriptionRefreshInterval <= 0 {
		return fmt.Errorf("SIDECAR_SUBSCRIPTION_REFRESH_INTERVAL must be positive")
	}
	return nil
}

func getenv(name, fallback string) string {
	if value, exists := os.LookupEnv(name); exists {
		return strings.TrimSpace(value)
	}
	return fallback
}

func duration(name string, fallback time.Duration) (time.Duration, error) {
	value, exists := os.LookupEnv(name)
	if !exists || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s must be a Go duration: %w", name, err)
	}
	return parsed, nil
}

func secondsOrDuration(name string, fallback time.Duration) (time.Duration, error) {
	value, exists := os.LookupEnv(name)
	if !exists || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	text := strings.TrimSpace(value)
	if seconds, err := strconv.ParseFloat(text, 64); err == nil {
		if seconds <= 0 {
			return 0, fmt.Errorf("%s must be positive", name)
		}
		return time.Duration(seconds * float64(time.Second)), nil
	}
	parsed, err := time.ParseDuration(text)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive seconds or a Go duration", name)
	}
	return parsed, nil
}

func positiveInt(name string, fallback int) (int, error) {
	value, exists := os.LookupEnv(name)
	if !exists || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}
