package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/WOLFraMix/aranea/internal/api"
	"github.com/WOLFraMix/aranea/internal/config"
	"github.com/WOLFraMix/aranea/internal/fetcher"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.New()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})

	db, err := sql.Open("sqlite",
		cfg.SQLitePath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		log.Fatalf("api: open db: %v", err)
	}
	// Один коннект, как в indexer'е: читатели в WAL и так конкурентны,
	// а редкие записи (INSERT краула, UPDATE статуса) защищены busy_timeout.
	db.SetMaxOpenConns(1)
	// sql.Open не подключается — реальные ошибки (битый путь, права)
	// ловим здесь, на старте, а не на первом запросе.
	if err := db.PingContext(context.Background()); err != nil {
		log.Fatalf("api: ping db: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS crawls (
			id         INTEGER PRIMARY KEY,
			url        TEXT NOT NULL,
			depth      INTEGER NOT NULL,
			status     TEXT NOT NULL DEFAULT 'running',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`); err != nil {
		log.Fatalf("api: schema: %v", err)
	}
	defer db.Close()

	// Счётчик не должен раздавать номера, уже занятые legacy-краулами
	// в базе (эра MAX+1): если Redis отстаёт от MAX(crawl_id) — подтягиваем.
	var maxID int64
	if err := db.QueryRow(`SELECT COALESCE(MAX(crawl_id), 0) FROM pages`).Scan(&maxID); err == nil && maxID > 0 {
		ctxSync, cancelSync := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancelSync()
		if v, err := rdb.Get(ctxSync, "aranea:crawl_seq").Int64(); err != nil || v < maxID {
			rdb.Set(ctxSync, "aranea:crawl_seq", maxID, 0) // следующий INCR даст maxID+1
		}
	}

	prod, err := fetcher.NewProducer(cfg.KafkaAddr)
	if err != nil {
		log.Fatalf("api: producer: %v", err)
	}
	defer prod.Close()

	srv := api.New(db, prod, rdb, cfg.MaxDepth)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("api: listening on %s", cfg.HTTPAddr)
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
