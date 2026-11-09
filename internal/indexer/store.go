// Package indexer — дедупликация ссылок, хранение страниц и маршрутизация.
package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	_ "modernc.org/sqlite" // чистый Go-драйвер: важен наш CGO_ENABLED=0 в Dockerfile

	"github.com/WOLFraMix/aranea/internal/event"
)

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
	DROP INDEX IF EXISTS idx_pages_url;
	CREATE UNIQUE INDEX IF NOT EXISTS idx_pages_crawl_url ON pages(crawl_id, url);
		CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(url, title, text);
	CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
		INSERT INTO pages_fts(rowid, url, title, text)
		VALUES (new.id, new.url, COALESCE(new.title, ''), COALESCE(new.text, ''));
	END;
	`
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}
	// Миграция: колонка text для полнотекстового поиска.
	// SQLite не умеет ADD COLUMN IF NOT EXISTS — игнорируем «уже есть».
	if _, err := db.Exec(`ALTER TABLE pages ADD COLUMN text TEXT`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return nil, fmt.Errorf("migrate text column: %w", err)
		}
	}
	// Пересоздаём FTS, только если индекс старой формы (без колонки text).
	// Пересоздание включает полный бэкпорт из pages — идемпотентно для свежих строк.
	var ftsSQL string
	if err := db.QueryRow(
		`SELECT COALESCE(sql, '') FROM sqlite_master WHERE name = 'pages_fts'`).Scan(&ftsSQL); err == nil &&
		!strings.Contains(ftsSQL, "text") {
		if _, err := db.Exec(`
			DROP TABLE IF EXISTS pages_fts;
			CREATE VIRTUAL TABLE pages_fts USING fts5(url, title, text);
			CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
				INSERT INTO pages_fts(rowid, url, title, text)
				VALUES (new.id, new.url, COALESCE(new.title, ''), COALESCE(new.text, ''));
			END;`); err != nil {
			return nil, fmt.Errorf("fts rebuild: %w", err)
		}
	}
	return &Store{rdb: rdb, db: db}, nil
}

// seenKey делает ключ дедупа на каждый краул: атомарный SADD работает
// в пределах одного краула, разные краулы не засоряют друг друга.
func seenKey(crawlID int64) string {
	return fmt.Sprintf("aranea:seen:%d", crawlID)
}

// Visit атомарно отмечает URL как виденный В РАМКАХ краула crawlID.
// Возвращает true, если мы первые — как раньше, но теперь per-crawl.
func (s *Store) Visit(ctx context.Context, crawlID int64, rawURL string) (bool, error) {
	n, err := s.rdb.SAdd(ctx, seenKey(crawlID), rawURL).Result()
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
			(crawl_id, url, title, status_code, latency_ms, depth, error, text)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.CrawlID, r.URL, r.Title, r.StatusCode, r.LatencyMs, r.Depth, r.Error, r.Text,
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

// Cancelled сообщает, запрошена ли отмена краула.
func (s *Store) Cancelled(ctx context.Context, crawlID int64) bool {
	n, err := s.rdb.Exists(ctx, event.CancelKey(crawlID)).Result()
	return err == nil && n > 0 // при ошибке Redis краул не глохнет — at-least-once продолжит
}

// CountPublished отмечает публикацию новой задачи краула (seed или ребёнок).
// Счётчики с TTL: через сутки после смерти краула они исчезают сами.
func (s *Store) CountPublished(ctx context.Context, crawlID int64) error {
	key := fmt.Sprintf("aranea:published:%d", crawlID)
	if err := s.rdb.Incr(ctx, key).Err(); err != nil {
		return fmt.Errorf("incr published: %w", err)
	}
	s.rdb.Expire(ctx, key, 24*time.Hour)
	return nil
}

// CountProcessed отмечает обработанную страницу — вызывается после Save.
func (s *Store) CountProcessed(ctx context.Context, crawlID int64) error {
	key := fmt.Sprintf("aranea:processed:%d", crawlID)
	if err := s.rdb.Incr(ctx, key).Err(); err != nil {
		return fmt.Errorf("incr processed: %w", err)
	}
	s.rdb.Expire(ctx, key, 24*time.Hour)
	return nil
}
