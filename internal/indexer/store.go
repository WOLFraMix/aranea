// Package indexer — дедупликация ссылок, хранение страниц и маршрутизация.
package indexer

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/redis/go-redis/v9"
	_ "modernc.org/sqlite" // чистый Go-драйвер: важен наш CGO_ENABLED=0 в Dockerfile

	"github.com/WOLFraMix/aranea/internal/event"
)

const seenKey = "aranea:seen" // SET всех встреченных URL

// Store — дедуп через Redis и хранение страниц в SQLite.
type Store struct {
	rdb *redis.Client
	db  *sql.DB
}

// NewStore открывает Redis и SQLite и гарантирует схему таблицы.
func NewStore(redisAddr, sqlitePath string) (*Store, error) {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	db, err := sql.Open("sqlite",
		sqlitePath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, fmt.Errorf("sqlite: %w", err)
	}
	// SQLite в конкурентном доступе капризен: один коннект = одна запись за раз.
	// Для нас этого достаточно — записывает только indexer.
	db.SetMaxOpenConns(1)

	schema := `
	CREATE TABLE IF NOT EXISTS pages (
		id         INTEGER PRIMARY KEY,
		crawl_id   INTEGER NOT NULL,
		url        TEXT NOT NULL,
		title      TEXT,
		status_code INTEGER,
		latency_ms  INTEGER,
		depth      INTEGER,
		error      TEXT,
		fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_pages_url ON pages(url);
	`
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Store{rdb: rdb, db: db}, nil
}

// Visit атомарно отмечает URL как виденный. Возвращает true, если мы первые:
// SADD — тот самый атомарный test-and-set, который чинит race condition.
func (s *Store) Visit(ctx context.Context, rawURL string) (bool, error) {
	n, err := s.rdb.SAdd(ctx, seenKey, rawURL).Result()
	if err != nil {
		return false, fmt.Errorf("sadd: %w", err)
	}
	return n == 1, nil
}

// Save кладёт результат пробы в SQLite. Ошибочный ответ тоже сохраняем —
// для краула «не скачалось» это валидный факт, а не отсутствие данных.
func (s *Store) Save(ctx context.Context, r event.PageResult) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO pages
			(crawl_id, url, title, status_code, latency_ms, depth, error)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.CrawlID, r.URL, r.Title, r.StatusCode, r.LatencyMs, r.Depth, r.Error,
	)
	if err != nil {
		return fmt.Errorf("save page: %w", err)
	}
	return nil
}

func (s *Store) Close() {
	s.rdb.Close()
	s.db.Close()
}
