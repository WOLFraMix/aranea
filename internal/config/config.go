// Package config собирает конфигурацию всех сервисов из окружения.
package config

import (
	"log"
	"os"
	"strconv"
	"time"
)

// Config — настройки трёх сервисов проекта.
type Config struct {
	HTTPAddr     string        // api: адрес HTTP-сервера, ":8080"
	KafkaAddr    string        // брокер: "127.0.0.1:9093"
	RedisAddr    string        // Redis: "127.0.0.1:6379"
	SQLitePath   string        // файл базы: "aranea.db"
	Workers      int           // fetcher: размер пула воркеров
	MaxDepth     int           // ограничение глубины обхода по умолчанию
	MaxPerDomain int           // максимум страниц на один домен в обходе
	RatePerSec   int           // вежливость: запросов в секунду на домен
	ProbeTimeout time.Duration // таймаут одного HTTP GET
}

// New читает переменные окружения с fallback на дефолты.
func New() Config {
	return Config{
		HTTPAddr:     getEnv("HTTP_ADDR", ":8080"),
		KafkaAddr:    getEnv("KAFKA_ADDR", "127.0.0.1:9093"), // OUTSIDE-слушатель для клиентов с хоста
		RedisAddr:    getEnv("REDIS_ADDR", "127.0.0.1:6379"),
		SQLitePath:   getEnv("DB_PATH", "aranea.db"),
		Workers:      getInt("WORKERS", 16),
		MaxDepth:     getInt("MAX_DEPTH", 3),
		MaxPerDomain: getInt("MAX_PER_DOMAIN", 200),
		RatePerSec:   getInt("RATE_PER_SEC", 2),
		ProbeTimeout: getDuration("PROBE_TIMEOUT", 10*time.Second),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	s := os.Getenv(key)
	if s == "" {
		return def // переменной нет — дефолт, это норма, а не ошибка
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("config: invalid %s=%q: %v", key, s, err)
	}
	return v
}

func getDuration(key string, def time.Duration) time.Duration {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		log.Fatalf("config: invalid %s=%q: %v", key, s, err)
	}
	return v
}
