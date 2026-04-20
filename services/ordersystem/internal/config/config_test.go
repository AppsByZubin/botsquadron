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

func TestLoadDefaultsToSandbox(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@example.com:5432/omsdb?sslmode=disable")
	t.Setenv("UPSTOX_SANDBOX_API_ACCESS_TOKEN", "sandbox-token")
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
