package config

import (
	"os"
	"testing"
	"time"
)

var configurationEnvironment = []string{
	"SIDECAR_HTTP_ADDR", "SIDECAR_WEIGHTS_PATH", "SIDECAR_OUTPUT_PATH",
	"APP_TIMEZONE", "SIDECAR_INDEX_INSTRUMENT_KEY", "SIDECAR_BOT_ID",
	"SIDECAR_MARKET_OPEN", "SIDECAR_MARKET_CLOSE", "SIDECAR_FINALIZE_GRACE",
	"SIDECAR_MAX_SNAPSHOTS", "NATS_URL", "SIDECAR_NATS_TICK_SUBJECT",
	"SIDECAR_NATS_INSTRUMENT_SUBJECT", "NATS_CONNECT_RETRY_WAIT_SEC",
	"NATS_CONNECT_TIMEOUT_SEC", "SIDECAR_SUBSCRIPTION_REFRESH_INTERVAL",
	"SIDECAR_HTTP_READ_HEADER_TIMEOUT", "SIDECAR_HTTP_READ_TIMEOUT",
	"SIDECAR_HTTP_WRITE_TIMEOUT", "SIDECAR_HTTP_IDLE_TIMEOUT", "SIDECAR_SHUTDOWN_TIMEOUT",
}

func TestLoadDefaults(t *testing.T) {
	clearConfigurationEnvironment(t)
	settings, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if settings.HTTPAddr != ":8082" || settings.TimezoneName != "Asia/Kolkata" {
		t.Fatalf("unexpected HTTP/timezone defaults: %+v", settings)
	}
	if settings.FinalizeGrace != 2*time.Second || settings.MaximumSnapshots != 600 {
		t.Fatalf("unexpected calculation defaults: %+v", settings)
	}
	if settings.NATSReconnectWait != 2*time.Second || settings.NATSConnectTimeout != 5*time.Second {
		t.Fatalf("unexpected NATS defaults: %+v", settings)
	}
}

func TestLoadAcceptsSecondsAndDurationRetrySettings(t *testing.T) {
	clearConfigurationEnvironment(t)
	t.Setenv("NATS_CONNECT_RETRY_WAIT_SEC", "0.25")
	t.Setenv("NATS_CONNECT_TIMEOUT_SEC", "1500ms")
	t.Setenv("SIDECAR_FINALIZE_GRACE", "3s")
	settings, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if settings.NATSReconnectWait != 250*time.Millisecond {
		t.Fatalf("ReconnectWait = %s", settings.NATSReconnectWait)
	}
	if settings.NATSConnectTimeout != 1500*time.Millisecond {
		t.Fatalf("ConnectTimeout = %s", settings.NATSConnectTimeout)
	}
	if settings.FinalizeGrace != 3*time.Second {
		t.Fatalf("FinalizeGrace = %s", settings.FinalizeGrace)
	}
}

func TestLoadRejectsInvalidSettings(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "HTTP address", key: "SIDECAR_HTTP_ADDR", value: "8082"},
		{name: "finalize grace", key: "SIDECAR_FINALIZE_GRACE", value: "1m"},
		{name: "refresh interval", key: "SIDECAR_SUBSCRIPTION_REFRESH_INTERVAL", value: "0s"},
		{name: "snapshot maximum", key: "SIDECAR_MAX_SNAPSHOTS", value: "0"},
		{name: "timezone", key: "APP_TIMEZONE", value: "Mars/Olympus"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfigurationEnvironment(t)
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil {
				t.Fatalf("Load() accepted %s=%q", test.key, test.value)
			}
		})
	}
}

func clearConfigurationEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range configurationEnvironment {
		value, exists := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
		name, value, exists := name, value, exists
		t.Cleanup(func() {
			if exists {
				_ = os.Setenv(name, value)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
}
