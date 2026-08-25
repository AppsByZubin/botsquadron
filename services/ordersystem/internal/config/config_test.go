package config

import (
	"strings"
	"testing"
)

func TestLoadSelectsProductionUpstoxBaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("APP_MODE", "production")
	t.Setenv("UPSTOX_API_ACCESS_TOKEN", "prod-token")
	t.Setenv("upstox_api_access_token", "")
	t.Setenv("UPSTOX_API_BASE_URL", "https://prod.example.com/")
	t.Setenv("UPSTOX_SANDBOX_API_BASE_URL", "https://sandbox.example.com/")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.AppMode != ModeProduction {
		t.Fatalf("AppMode = %q, want %q", cfg.AppMode, ModeProduction)
	}
	if cfg.UpstoxBaseURL != "https://prod.example.com" {
		t.Fatalf("UpstoxBaseURL = %q, want production base URL", cfg.UpstoxBaseURL)
	}
	if cfg.UpstoxAccessToken != "prod-token" {
		t.Fatalf("UpstoxAccessToken = %q, want prod-token", cfg.UpstoxAccessToken)
	}
}

func TestLoadSelectsSandboxUpstoxBaseURLAndToken(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("APP_MODE", "sandbox")
	t.Setenv("UPSTOX_SANDBOX_API_ACCESS_TOKEN", "")
	t.Setenv("upstox_sandbox_api_access_token", "sandbox-token")
	t.Setenv("UPSTOX_API_BASE_URL", "https://prod.example.com/")
	t.Setenv("UPSTOX_SANDBOX_API_BASE_URL", "https://sandbox.example.com/")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.AppMode != ModeSandbox {
		t.Fatalf("AppMode = %q, want %q", cfg.AppMode, ModeSandbox)
	}
	if cfg.UpstoxBaseURL != "https://sandbox.example.com" {
		t.Fatalf("UpstoxBaseURL = %q, want sandbox base URL", cfg.UpstoxBaseURL)
	}
	if cfg.UpstoxAccessToken != "sandbox-token" {
		t.Fatalf("UpstoxAccessToken = %q, want sandbox-token", cfg.UpstoxAccessToken)
	}
}

func TestLoadDefaultsUpstoxV2EndpointsToAPIHost(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("APP_MODE", "production")
	t.Setenv("UPSTOX_API_ACCESS_TOKEN", "prod-token")
	t.Setenv("UPSTOX_API_BASE_URL", "https://api-hft.upstox.com")
	t.Setenv("UPSTOX_EXIT_POSITIONS_PATH", "")
	t.Setenv("UPSTOX_POSITIONS_PATH", "")
	t.Setenv("UPSTOX_ORDER_DETAILS_PATH", "")
	t.Setenv("UPSTOX_ORDER_TRADES_PATH", "")
	t.Setenv("UPSTOX_BROKERAGE_PATH", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.UpstoxBaseURL != "https://api-hft.upstox.com" {
		t.Fatalf("UpstoxBaseURL = %q, want HFT base URL", cfg.UpstoxBaseURL)
	}
	if cfg.UpstoxExitPositionsPath != "https://api.upstox.com/v2/order/positions/exit" {
		t.Fatalf("UpstoxExitPositionsPath = %q, want api.upstox.com absolute URL", cfg.UpstoxExitPositionsPath)
	}
	if cfg.UpstoxPositionsPath != "https://api.upstox.com/v2/portfolio/short-term-positions" {
		t.Fatalf("UpstoxPositionsPath = %q, want api.upstox.com absolute URL", cfg.UpstoxPositionsPath)
	}
	if cfg.UpstoxOrderDetailsPath != "https://api.upstox.com/v2/order/details" {
		t.Fatalf("UpstoxOrderDetailsPath = %q, want api.upstox.com absolute URL", cfg.UpstoxOrderDetailsPath)
	}
	if cfg.UpstoxOrderTradesPath != "https://api.upstox.com/v2/order/trades" {
		t.Fatalf("UpstoxOrderTradesPath = %q, want api.upstox.com absolute URL", cfg.UpstoxOrderTradesPath)
	}
	if cfg.UpstoxBrokeragePath != "https://api.upstox.com/v2/charges/brokerage" {
		t.Fatalf("UpstoxBrokeragePath = %q, want api.upstox.com absolute URL", cfg.UpstoxBrokeragePath)
	}
}

func TestLoadForcesProductionEndpointFQDNs(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("APP_MODE", "production")
	t.Setenv("UPSTOX_API_ACCESS_TOKEN", "prod-token")
	t.Setenv("UPSTOX_API_BASE_URL", "https://api-hft.upstox.com")
	t.Setenv("UPSTOX_ORDER_PLACE_PATH", "/v3/order/place")
	t.Setenv("UPSTOX_ORDER_MODIFY_PATH", "https://api.upstox.com/v3/order/modify")
	t.Setenv("UPSTOX_ORDER_CANCEL_PATH", "/v3/order/cancel")
	t.Setenv("UPSTOX_EXIT_POSITIONS_PATH", "/v2/order/positions/exit")
	t.Setenv("UPSTOX_POSITIONS_PATH", "/v2/portfolio/short-term-positions")
	t.Setenv("UPSTOX_ORDER_DETAILS_PATH", "/v2/order/details")
	t.Setenv("UPSTOX_ORDER_TRADES_PATH", "https://api-hft.upstox.com/v2/order/trades")
	t.Setenv("UPSTOX_BROKERAGE_PATH", "/v2/charges/brokerage")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.UpstoxOrderPlacePath != "https://api-hft.upstox.com/v3/order/place" {
		t.Fatalf("UpstoxOrderPlacePath = %q, want api-hft.upstox.com", cfg.UpstoxOrderPlacePath)
	}
	if cfg.UpstoxOrderModifyPath != "https://api-hft.upstox.com/v3/order/modify" {
		t.Fatalf("UpstoxOrderModifyPath = %q, want api-hft.upstox.com", cfg.UpstoxOrderModifyPath)
	}
	if cfg.UpstoxOrderCancelPath != "https://api-hft.upstox.com/v3/order/cancel" {
		t.Fatalf("UpstoxOrderCancelPath = %q, want api-hft.upstox.com", cfg.UpstoxOrderCancelPath)
	}
	if cfg.UpstoxExitPositionsPath != "https://api.upstox.com/v2/order/positions/exit" {
		t.Fatalf("UpstoxExitPositionsPath = %q, want api.upstox.com", cfg.UpstoxExitPositionsPath)
	}
	if cfg.UpstoxPositionsPath != "https://api.upstox.com/v2/portfolio/short-term-positions" {
		t.Fatalf("UpstoxPositionsPath = %q, want api.upstox.com", cfg.UpstoxPositionsPath)
	}
	if cfg.UpstoxOrderDetailsPath != "https://api.upstox.com/v2/order/details" {
		t.Fatalf("UpstoxOrderDetailsPath = %q, want api.upstox.com", cfg.UpstoxOrderDetailsPath)
	}
	if cfg.UpstoxOrderTradesPath != "https://api.upstox.com/v2/order/trades" {
		t.Fatalf("UpstoxOrderTradesPath = %q, want api.upstox.com", cfg.UpstoxOrderTradesPath)
	}
	if cfg.UpstoxBrokeragePath != "https://api.upstox.com/v2/charges/brokerage" {
		t.Fatalf("UpstoxBrokeragePath = %q, want api.upstox.com", cfg.UpstoxBrokeragePath)
	}
}

func TestLoadDefaultsToSandbox(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("UPSTOX_SANDBOX_API_ACCESS_TOKEN", "sandbox-token")
	t.Setenv("UPSTOX_SANDBOX_API_BASE_URL", "https://sandbox.example.com/")
	t.Setenv("UPSTOX_ORDER_DETAILS_PATH", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.AppMode != ModeSandbox {
		t.Fatalf("AppMode = %q, want %q", cfg.AppMode, ModeSandbox)
	}
	if cfg.UpstoxBaseURL != "https://sandbox.example.com" {
		t.Fatalf("UpstoxBaseURL = %q, want sandbox base URL", cfg.UpstoxBaseURL)
	}
	if cfg.UpstoxOrderDetailsPath != "/v2/order/details" {
		t.Fatalf("UpstoxOrderDetailsPath = %q, want sandbox-relative path", cfg.UpstoxOrderDetailsPath)
	}
}

func TestLoadReadsLossThresholds(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("UPSTOX_SANDBOX_API_ACCESS_TOKEN", "sandbox-token")
	t.Setenv("threshold_day_loss", "3500.75")
	t.Setenv("threshold_month_loss", "12500.50")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.ThresholdDayLoss != 3500.75 {
		t.Fatalf("ThresholdDayLoss = %v, want 3500.75", cfg.ThresholdDayLoss)
	}
	if cfg.ThresholdMonthLoss != 12500.50 {
		t.Fatalf("ThresholdMonthLoss = %v, want 12500.50", cfg.ThresholdMonthLoss)
	}
}

func TestLoadReadsStrategyBotNames(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("UPSTOX_SANDBOX_API_ACCESS_TOKEN", "sandbox-token")
	t.Setenv("ORDERSYSTEM_STRATEGY_BOT_NAMES", "alpha, beta,alpha")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	want := []string{"alpha", "beta"}
	if len(cfg.StrategyBotNames) != len(want) {
		t.Fatalf("StrategyBotNames = %#v, want %#v", cfg.StrategyBotNames, want)
	}
	for idx := range want {
		if cfg.StrategyBotNames[idx] != want[idx] {
			t.Fatalf("StrategyBotNames = %#v, want %#v", cfg.StrategyBotNames, want)
		}
	}
}

func TestLoadRejectsUnsupportedMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("APP_MODE", "paper")

	_, err := Load()
	if err == nil {
		t.Fatal("Load returned nil error, want mode validation error")
	}
	if !strings.Contains(err.Error(), "APP_MODE must be sandbox or production") {
		t.Fatalf("error = %q, want mode validation error", err.Error())
	}
}

func TestLoadRequiresSandboxToken(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("APP_MODE", "sandbox")
	t.Setenv("UPSTOX_SANDBOX_API_ACCESS_TOKEN", "")
	t.Setenv("upstox_sandbox_api_access_token", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load returned nil error, want sandbox token error")
	}
	if !strings.Contains(err.Error(), "UPSTOX_SANDBOX_API_ACCESS_TOKEN") {
		t.Fatalf("error = %q, want sandbox token error", err.Error())
	}
}
