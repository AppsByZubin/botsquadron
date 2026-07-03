package config

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr                string
	DatabaseURL             string
	AppMode                 string
	AppTimezone             string
	RequestTimeout          time.Duration
	SLPollInterval          time.Duration
	SLRefreshMinInterval    time.Duration
	AccountInitialCash      float64
	ThresholdDayLoss        float64
	StrategyBotNames        []string
	UpstoxBaseURL           string
	UpstoxAccessToken       string
	UpstoxOrderPlacePath    string
	UpstoxOrderModifyPath   string
	UpstoxOrderCancelPath   string
	UpstoxExitPositionsPath string
	UpstoxOrderDetailsPath  string
	UpstoxOrderTradesPath   string
	UpstoxBrokeragePath     string
	UpstoxAPIVersion        string
	UpstoxOrderRequestGap   time.Duration
	UpstoxStatusRequestGap  time.Duration
	UpstoxStatusCacheTTL    time.Duration
}

const (
	ModeSandbox    = "sandbox"
	ModeProduction = "production"

	upstoxStandardBaseURL = "https://api.upstox.com"
	upstoxHFTBaseURL      = "https://api-hft.upstox.com"
)

func Load() (Config, error) {
	appMode := normalizeAppMode(getEnv("APP_MODE", ModeSandbox))
	cfg := Config{
		HTTPAddr:                getEnv("ORDERSYSTEM_HTTP_ADDR", ":8081"),
		DatabaseURL:             strings.TrimSpace(os.Getenv("DATABASE_URL")),
		AppMode:                 appMode,
		AppTimezone:             strings.TrimSpace(getEnv("APP_TIMEZONE", "Asia/Kolkata")),
		RequestTimeout:          parseDurationEnv("ORDERSYSTEM_REQUEST_TIMEOUT", 15*time.Second),
		SLPollInterval:          parseDurationEnv("ORDERSYSTEM_SL_POLL_INTERVAL", 10*time.Second),
		SLRefreshMinInterval:    parseDurationEnv("ORDERSYSTEM_SL_REFRESH_MIN_INTERVAL", 10*time.Second),
		AccountInitialCash:      parseFloatEnv("ACCOUNT_INITIAL_CASH", 0),
		ThresholdDayLoss:        parseFloatEnvAny([]string{"threshold_day_loss", "THRESHOLD_DAY_LOSS", "ORDERSYSTEM_THRESHOLD_DAY_LOSS"}, 0),
		StrategyBotNames:        resolveStrategyBotNames(),
		UpstoxBaseURL:           resolveUpstoxBaseURL(appMode),
		UpstoxAccessToken:       resolveUpstoxAccessToken(appMode),
		UpstoxOrderPlacePath:    resolveUpstoxEndpoint(appMode, "UPSTOX_ORDER_PLACE_PATH", upstoxHFTBaseURL, "/v3/order/place"),
		UpstoxOrderModifyPath:   resolveUpstoxEndpoint(appMode, "UPSTOX_ORDER_MODIFY_PATH", upstoxHFTBaseURL, "/v3/order/modify"),
		UpstoxOrderCancelPath:   resolveUpstoxEndpoint(appMode, "UPSTOX_ORDER_CANCEL_PATH", upstoxHFTBaseURL, "/v3/order/cancel"),
		UpstoxExitPositionsPath: resolveUpstoxEndpoint(appMode, "UPSTOX_EXIT_POSITIONS_PATH", upstoxStandardBaseURL, "/v2/order/positions/exit"),
		UpstoxOrderDetailsPath:  resolveUpstoxEndpoint(appMode, "UPSTOX_ORDER_DETAILS_PATH", upstoxStandardBaseURL, "/v2/order/details"),
		UpstoxOrderTradesPath:   resolveUpstoxEndpoint(appMode, "UPSTOX_ORDER_TRADES_PATH", upstoxStandardBaseURL, "/v2/order/trades"),
		UpstoxBrokeragePath:     resolveUpstoxEndpoint(appMode, "UPSTOX_BROKERAGE_PATH", upstoxStandardBaseURL, "/v2/charges/brokerage"),
		UpstoxAPIVersion:        strings.TrimSpace(getEnv("UPSTOX_API_VERSION", "2.0")),
		UpstoxOrderRequestGap:   parseDurationEnv("ORDERSYSTEM_UPSTOX_ORDER_REQUEST_GAP", 750*time.Millisecond),
		UpstoxStatusRequestGap:  parseDurationEnv("ORDERSYSTEM_UPSTOX_STATUS_REQUEST_GAP", 750*time.Millisecond),
		UpstoxStatusCacheTTL:    parseDurationEnv("ORDERSYSTEM_UPSTOX_STATUS_CACHE_TTL", 5*time.Second),
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

	if math.IsNaN(cfg.ThresholdDayLoss) || math.IsInf(cfg.ThresholdDayLoss, 0) || cfg.ThresholdDayLoss < 0 {
		return Config{}, fmt.Errorf("threshold_day_loss must be >= 0")
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
	return strings.TrimRight(getEnv("UPSTOX_API_BASE_URL", upstoxStandardBaseURL), "/")
}

func resolveUpstoxEndpoint(appMode string, envKey string, productionBaseURL string, defaultPath string) string {
	value := strings.TrimSpace(os.Getenv(envKey))
	if value == "" {
		value = defaultPath
	}
	if normalizeAppMode(appMode) != ModeProduction {
		return normalizePath(value)
	}
	return strings.TrimRight(productionBaseURL, "/") + endpointPath(value)
}

func endpointPath(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://") {
		parsed, err := url.Parse(value)
		if err == nil {
			path := normalizePath(parsed.EscapedPath())
			if parsed.RawQuery != "" {
				path += "?" + parsed.RawQuery
			}
			return path
		}
	}
	return normalizePath(value)
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

func parseFloatEnvAny(keys []string, fallback float64) float64 {
	for _, key := range keys {
		value := strings.TrimSpace(os.Getenv(key))
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fallback
		}
		return parsed
	}
	return fallback
}

func resolveStrategyBotNames() []string {
	if configured := getFirstEnv("ORDERSYSTEM_STRATEGY_BOT_NAMES", "ORDERSYSTEM_KILL_SWITCH_BOT_NAMES"); configured != "" {
		return splitCleanCSV(configured)
	}

	names := []string{
		getEnv("SOLOBOT_BOT_NAME", "solobot"),
		getEnv("TRENDOBOT_BOT_NAME", "trendobot"),
		getEnv("HAEMABOT_BOT_NAME", "haemabot"),
		getEnv("FIREBOT_BOT_NAME", "firebot"),
		getEnv("TITANBOT_BOT_NAME", "titanbot"),
		getEnv("FIBOBOT_BOT_NAME", "fibobot"),
	}
	return cleanStringSet(names)
}

func splitCleanCSV(value string) []string {
	parts := strings.Split(value, ",")
	return cleanStringSet(parts)
}

func cleanStringSet(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		seen := false
		for _, existing := range out {
			if existing == value {
				seen = true
				break
			}
		}
		if !seen {
			out = append(out, value)
		}
	}
	return out
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
