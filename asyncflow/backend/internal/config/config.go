package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds engine tuning parameters, all overridable via environment.
type Config struct {
	HTTPAddr string

	PostgresDSN string
	RedisAddr   string

	// Fairness: after HighWatermark consecutive non-bulk pops, force a
	// lower-priority pop when one is available.
	FairnessHighWatermark int

	// Scheduler / delay timing.
	DispatchInterval  time.Duration
	DelayScanInterval time.Duration

	// Leases.
	LeaseSeconds         int
	HeartbeatTimeoutSecs int
	ReaperInterval       time.Duration

	// Embedded worker.
	EmbeddedWorkerEnabled bool
	EmbeddedWorkerSlots   int
	EmbeddedWorkerName    string
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// Load reads configuration from the environment with sensible defaults.
func Load() Config {
	return Config{
		HTTPAddr:              getenv("HTTP_ADDR", ":8080"),
		PostgresDSN:           getenv("POSTGRES_DSN", "postgres://asyncflow:asyncflow@postgres:5432/asyncflow?sslmode=disable"),
		RedisAddr:             getenv("REDIS_ADDR", "redis:6379"),
		FairnessHighWatermark: getenvInt("FAIRNESS_HIGH_WATERMARK", 5),
		DispatchInterval:      getenvDur("DISPATCH_INTERVAL", 50*time.Millisecond),
		DelayScanInterval:     getenvDur("DELAY_SCAN_INTERVAL", 250*time.Millisecond),
		LeaseSeconds:          getenvInt("LEASE_SECONDS", 30),
		HeartbeatTimeoutSecs:  getenvInt("HEARTBEAT_TIMEOUT_SECS", 15),
		ReaperInterval:        getenvDur("REAPER_INTERVAL", 2*time.Second),
		EmbeddedWorkerEnabled: getenv("EMBEDDED_WORKER_ENABLED", "true") == "true",
		EmbeddedWorkerSlots:   getenvInt("EMBEDDED_WORKER_SLOTS", 4),
		EmbeddedWorkerName:    getenv("EMBEDDED_WORKER_NAME", "embedded-1"),
	}
}
