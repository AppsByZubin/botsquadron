package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr               string
	DatabaseURL            string
	AppMode                string
	AppTimezone            string
	RequestTimeout         time.Duration
	SLPollInterval         time.Duration
	SLRefreshMinInterval   time.Duration
	AccountInitialCash     float64
	UpstoxBaseURL          string
	UpstoxAccessToken      string
	UpstoxOrderPlacePath   string
	UpstoxOrderModifyPath  string
	UpstoxOrderDetailsPath string
	UpstoxOrderTradesPath  string
	UpstoxBrokeragePath    string
	UpstoxAPIVersion       string
	UpstoxOrderRequestGap  time.Duration
	UpstoxStatusRequestGap time.Duration
	UpstoxStatusCacheTTL   time.Duration
}

const (
	ModeSandbox    = "sandbox"
	ModeProduction = "production"
)

func Load() (Config, error) {
	appMode := normalizeAppMode(getEnv("APP_MODE", ModeSandbox))
	cfg := Config{
		HTTPAddr:               getEnv("ORDERSYSTEM_HTTP_ADDR", ":8081"),
		DatabaseURL:            strings.TrimSpace(os.Getenv("DATABASE_URL")),
		AppMode:                appMode,
		AppTimezone:            strings.TrimSpace(getEnv("APP_TIMEZONE", "Asia/Kolkata")),
		RequestTimeout:         parseDurationEnv("ORDERSYSTEM_REQUEST_TIMEOUT", 15*time.Second),
		SLPollInterval:         parseDurationEnv("ORDERSYSTEM_SL_POLL_INTERVAL", 10*time.Second),
		SLRefreshMinInterval:   parseDurationEnv("ORDERSYSTEM_SL_REFRESH_MIN_INTERVAL", 10*time.Second),
		AccountInitialCash:     parseFloatEnv("ACCOUNT_INITIAL_CASH", 0),
		UpstoxBaseURL:          resolveUpstoxBaseURL(appMode),
		UpstoxAccessToken:      resolveUpstoxAccessToken(appMode),
		UpstoxOrderPlacePath:   normalizePath(getEnv("UPSTOX_ORDER_PLACE_PATH", "/v3/order/place")),
		UpstoxOrderModifyPath:  normalizePath(getEnv("UPSTOX_ORDER_MODIFY_PATH", "/v3/order/modify")),
		UpstoxOrderDetailsPath: normalizePath(getEnv("UPSTOX_ORDER_DETAILS_PATH", "/v2/order/details")),
		UpstoxOrderTradesPath:  normalizePath(getEnv("UPSTOX_ORDER_TRADES_PATH", "/v2/order/trades")),
		UpstoxBrokeragePath:    normalizePath(getEnv("UPSTOX_BROKERAGE_PATH", "/v2/charges/brokerage")),
		UpstoxAPIVersion:       strings.TrimSpace(getEnv("UPSTOX_API_VERSION", "2.0")),
		UpstoxOrderRequestGap:  parseDurationEnv("ORDERSYSTEM_UPSTOX_ORDER_REQUEST_GAP", 750*time.Millisecond),
		UpstoxStatusRequestGap: parseDurationEnv("ORDERSYSTEM_UPSTOX_STATUS_REQUEST_GAP", 750*time.Millisecond),
		UpstoxStatusCacheTTL:   parseDurationEnv("ORDERSYSTEM_UPSTOX_STATUS_CACHE_TTL", 5*time.Second),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}

	if !isValidAppMode(cfg.AppMode) {
		return Config{}, errors.New("APP_MODE must be sandbox or production")
	}

	if cfg.UsesUpstox() && cfg.UpstoxAccessToken == "" {
		if cfg.IsSandbox() {
			return Config{}, errors.New("UPSTOX_SANDBOX_API_ACCESS_TOKEN is required when APP_MODE=sandbox")
		}
		return Config{}, errors.New("UPSTOX_API_ACCESS_TOKEN is required when APP_MODE=production")
	}

	if cfg.SLPollInterval <= 0 {
		return Config{}, fmt.Errorf("ORDERSYSTEM_SL_POLL_INTERVAL must be > 0")
	}

	if cfg.SLRefreshMinInterval < 0 {
		return Config{}, fmt.Errorf("ORDERSYSTEM_SL_REFRESH_MIN_INTERVAL must be >= 0")
	}

	if cfg.RequestTimeout <= 0 {
		return Config{}, fmt.Errorf("ORDERSYSTEM_REQUEST_TIMEOUT must be > 0")
	}

	if cfg.UpstoxStatusRequestGap < 0 {
		return Config{}, fmt.Errorf("ORDERSYSTEM_UPSTOX_STATUS_REQUEST_GAP must be >= 0")
	}

	if cfg.UpstoxOrderRequestGap < 0 {
		return Config{}, fmt.Errorf("ORDERSYSTEM_UPSTOX_ORDER_REQUEST_GAP must be >= 0")
	}

	if cfg.UpstoxStatusCacheTTL < 0 {
		return Config{}, fmt.Errorf("ORDERSYSTEM_UPSTOX_STATUS_CACHE_TTL must be >= 0")
	}

	return cfg, nil
}

func (c Config) IsProduction() bool {
	return normalizeAppMode(c.AppMode) == ModeProduction
}

func (c Config) IsSandbox() bool {
	return normalizeAppMode(c.AppMode) == ModeSandbox
}

func (c Config) UsesUpstox() bool {
	return c.IsProduction() || c.IsSandbox()
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getFirstEnv(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

func normalizeAppMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "prod":
		return ModeProduction
	case ModeProduction:
		return ModeProduction
	case ModeSandbox:
		return ModeSandbox
	case "":
		return ModeSandbox
	default:
		return strings.ToLower(strings.TrimSpace(mode))
	}
}

func isValidAppMode(mode string) bool {
	switch normalizeAppMode(mode) {
	case ModeSandbox, ModeProduction:
		return true
	default:
		return false
	}
}

func resolveUpstoxBaseURL(appMode string) string {
	if normalizeAppMode(appMode) == ModeSandbox {
		return strings.TrimRight(getEnv("UPSTOX_SANDBOX_API_BASE_URL", "https://api-sandbox.upstox.com"), "/")
	}
	return strings.TrimRight(getEnv("UPSTOX_API_BASE_URL", "https://api.upstox.com"), "/")
}

func resolveUpstoxAccessToken(appMode string) string {
	if normalizeAppMode(appMode) == ModeSandbox {
		return getFirstEnv("UPSTOX_SANDBOX_API_ACCESS_TOKEN", "upstox_sandbox_api_access_token")
	}
	return getFirstEnv("UPSTOX_API_ACCESS_TOKEN", "upstox_api_access_token")
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return d
}

func parseFloatEnv(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func normalizePath(path string) string {
	clean := strings.TrimSpace(path)
	if clean == "" {
		return "/"
	}
	if strings.HasPrefix(clean, "http://") || strings.HasPrefix(clean, "https://") {
		return clean
	}
	if !strings.HasPrefix(clean, "/") {
		return "/" + clean
	}
	return clean
}
