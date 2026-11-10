package config

import (
	"os"
	"testing"
	"time"
)

func TestNewDefaults(t *testing.T) {
	// Очищаем все переменные, которые читает New()
	os.Unsetenv("HTTP_ADDR")
	os.Unsetenv("KAFKA_ADDR")
	os.Unsetenv("REDIS_ADDR")
	os.Unsetenv("DB_PATH")
	os.Unsetenv("WORKERS")
	os.Unsetenv("MAX_DEPTH")
	os.Unsetenv("RATE_PER_SEC")
	os.Unsetenv("PROBE_TIMEOUT")

	cfg := New()

	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.KafkaAddr != "127.0.0.1:9093" {
		t.Errorf("KafkaAddr = %q, want 127.0.0.1:9093", cfg.KafkaAddr)
	}
	if cfg.RedisAddr != "127.0.0.1:6379" {
		t.Errorf("RedisAddr = %q, want 127.0.0.1:6379", cfg.RedisAddr)
	}
	if cfg.SQLitePath != "aranea.db" {
		t.Errorf("SQLitePath = %q, want aranea.db", cfg.SQLitePath)
	}
	if cfg.Workers != 16 {
		t.Errorf("Workers = %d, want 16", cfg.Workers)
	}
	if cfg.MaxDepth != 3 {
		t.Errorf("MaxDepth = %d, want 3", cfg.MaxDepth)
	}
	if cfg.RatePerSec != 2 {
		t.Errorf("RatePerSec = %d, want 2", cfg.RatePerSec)
	}
	if cfg.ProbeTimeout != 10*time.Second {
		t.Errorf("ProbeTimeout = %v, want 10s", cfg.ProbeTimeout)
	}
}

func TestNewEnvOverrides(t *testing.T) {
	os.Setenv("HTTP_ADDR", ":9090")
	os.Setenv("KAFKA_ADDR", "kafka.local:9092")
	os.Setenv("REDIS_ADDR", "redis.local:6380")
	os.Setenv("DB_PATH", "/tmp/test.db")
	os.Setenv("WORKERS", "32")
	os.Setenv("MAX_DEPTH", "5")
	os.Setenv("RATE_PER_SEC", "10")
	os.Setenv("PROBE_TIMEOUT", "30s")

	defer func() {
		os.Unsetenv("HTTP_ADDR")
		os.Unsetenv("KAFKA_ADDR")
		os.Unsetenv("REDIS_ADDR")
		os.Unsetenv("DB_PATH")
		os.Unsetenv("WORKERS")
		os.Unsetenv("MAX_DEPTH")
		os.Unsetenv("RATE_PER_SEC")
		os.Unsetenv("PROBE_TIMEOUT")
	}()

	cfg := New()

	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
	}
	if cfg.KafkaAddr != "kafka.local:9092" {
		t.Errorf("KafkaAddr = %q, want kafka.local:9092", cfg.KafkaAddr)
	}
	if cfg.RedisAddr != "redis.local:6380" {
		t.Errorf("RedisAddr = %q, want redis.local:6380", cfg.RedisAddr)
	}
	if cfg.SQLitePath != "/tmp/test.db" {
		t.Errorf("SQLitePath = %q, want /tmp/test.db", cfg.SQLitePath)
	}
	if cfg.Workers != 32 {
		t.Errorf("Workers = %d, want 32", cfg.Workers)
	}
	if cfg.MaxDepth != 5 {
		t.Errorf("MaxDepth = %d, want 5", cfg.MaxDepth)
	}
	if cfg.RatePerSec != 10 {
		t.Errorf("RatePerSec = %d, want 10", cfg.RatePerSec)
	}
	if cfg.ProbeTimeout != 30*time.Second {
		t.Errorf("ProbeTimeout = %v, want 30s", cfg.ProbeTimeout)
	}
}

func TestNewEnvEmptyString(t *testing.T) {
	// Пустая строка в env — не перезаписывает дефолт
	os.Setenv("HTTP_ADDR", "")
	defer os.Unsetenv("HTTP_ADDR")

	cfg := New()
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr with empty env = %q, want :8080", cfg.HTTPAddr)
	}
}
