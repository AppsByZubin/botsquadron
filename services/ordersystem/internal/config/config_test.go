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
