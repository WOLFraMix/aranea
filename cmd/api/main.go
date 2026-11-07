package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/WOLFraMix/aranea/internal/api"
	"github.com/WOLFraMix/aranea/internal/fetcher"
	"github.com/redis/go-redis/v9"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	kafkaAddr := env("KAFKA_ADDR", "127.0.0.1:9093")
	redisAddr := env("REDIS_ADDR", "127.0.0.1:6379")
	dbPath := env("DB_PATH", "aranea.db")
	maxDepth, _ := strconv.Atoi(env("MAX_DEPTH", "3"))

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		log.Fatalf("api: open db: %v", err)
	}
	defer db.Close()

	prod, err := fetcher.NewProducer(kafkaAddr)
	if err != nil {
		log.Fatalf("api: producer: %v", err)
	}
	defer prod.Close()

	srv := api.New(db, prod, rdb, maxDepth)

	httpSrv := &http.Server{
		Addr:              ":8080",
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("api: listening on :8080")
		if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("api: %v", err)
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		log.Printf("api: shutdown: %v", err)
	}
	log.Printf("api: stopped")
}
