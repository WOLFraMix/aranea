package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/WOLFraMix/aranea/internal/event"
	"github.com/WOLFraMix/aranea/internal/fetcher"
)

type Server struct {
	db       *sql.DB
	prod     *fetcher.Producer
	rdb      *redis.Client // счётчик crawl_id: атомарный INCR, переживает пустые краулы
	maxDepth int
}

func New(db *sql.DB, prod *fetcher.Producer, rdb *redis.Client, maxDepth int) *Server {
	return &Server{db: db, prod: prod, rdb: rdb, maxDepth: maxDepth}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/crawls", s.startCrawl)
	mux.HandleFunc("GET /api/crawls/{id}/pages", s.crawlPages)
	mux.HandleFunc("GET /api/search", s.search)
	mux.HandleFunc("DELETE /api/crawls/{id}", s.cancelCrawl)
	mux.HandleFunc("GET /api/crawls", s.listCrawls)
	mux.HandleFunc("GET /api/debug/page", s.debugPage)
	return mux
}

func (s *Server) startCrawl(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL   string `json:"url"`
		Depth int    `json:"depth"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	u, err := url.Parse(req.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		httpError(w, http.StatusBadRequest, "url must be an absolute http(s) URL")
		return
	}
	if req.Depth < 0 || req.Depth > s.maxDepth {
		httpError(w, http.StatusBadRequest, "depth must be between 0 and "+strconv.Itoa(s.maxDepth))
		return
	}

	// crawl_id из Redis INCR: атомарный счётчик. Пустой краул номер
	// не «отменяет» — два одновременных старта гарантированно разойдутся.
	id, err := s.rdb.Incr(r.Context(), "aranea:crawl_seq").Result()
	if err != nil {
		httpError(w, http.StatusBadGateway, "redis: "+err.Error())
		return
	}

	if _, err := s.db.ExecContext(r.Context(),
		`INSERT INTO crawls (id, url, depth) VALUES (?, ?, ?)`,
		id, u.String(), req.Depth); err != nil {
		httpError(w, http.StatusInternalServerError, "db: "+err.Error())
		return
	}

	// Лимит глубины едет в событии: каждый краул ограничен своим depth,
	// а не глобальным MAX_DEPTH из конфига.
	t := event.Task{CrawlID: id, URL: u.String(), Depth: 0, MaxDepth: &req.Depth}
	if err := s.prod.Publish(r.Context(), event.TopicTasks, t); err != nil {
		httpError(w, http.StatusBadGateway, "publish: "+err.Error())
		return
	}

	// Seed сразу в дедуп-SET: после рестарта indexer'а краул не скачает
	// его повторно, а ссылки на seed с детей не отправят его в цикл.
	if err := s.rdb.SAdd(r.Context(), fmt.Sprintf("aranea:seen:%d", id), t.URL).Err(); err != nil {
		log.Printf("dedup seed %s: %v", t.URL, err) // не роняем запрос — SET пересоберётся из событий
	}

	s.rdb.Expire(r.Context(), fmt.Sprintf("aranea:seen:%d", id), event.SeenTTL)

	if err := s.rdb.Incr(r.Context(), fmt.Sprintf("aranea:published:%d", id)).Err(); err != nil {
		log.Printf("publish counter: %v", err)
	}
	s.rdb.Expire(r.Context(), fmt.Sprintf("aranea:published:%d", id), 24*time.Hour)

	writeJSON(w, http.StatusCreated, map[string]any{
		"crawl_id": id,
		"url":      t.URL,
		"depth":    req.Depth,
	})
}

func (s *Server) crawlPages(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad crawl id")
		return
	}
	limit := clampInt(r.URL.Query().Get("limit"), 1, 100, 20)
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	rows, err := s.db.Query(
		`SELECT url, status_code, latency_ms, COALESCE(title, ''), COALESCE(depth, 0)
		 FROM pages WHERE crawl_id = ? ORDER BY rowid LIMIT ? OFFSET ?`,
		id, limit, offset)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "db: "+err.Error())
		return
	}
	defer rows.Close()

	type page struct {
		URL        string `json:"url"`
		StatusCode int    `json:"status_code"`
		LatencyMs  int64  `json:"latency_ms"`
		Title      string `json:"title"`
		Depth      int    `json:"depth"`
	}
	pages := []page{}
	for rows.Next() {
		var p page
		if err := rows.Scan(&p.URL, &p.StatusCode, &p.LatencyMs, &p.Title, &p.Depth); err != nil {
			log.Printf("scan page: %v", err) // ошибка видна, а не съедена
			continue
		}
		pages = append(pages, p)
	}
	if err := rows.Err(); err != nil {
		httpError(w, http.StatusInternalServerError, "rows: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"crawl_id": id, "pages": pages})
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		httpError(w, http.StatusBadRequest, "q is required")
		return
	}
	// Каждый токен — в кавычки: у FTS5 есть свои операторы (AND, OR, NEAR,
	// скобки, кавычки), и пользовательский ввод не должен их активировать.
	parts := strings.Fields(q)
	if len(parts) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"query": q, "hits": []any{}})
		return
	}
	for i, p := range parts {
		parts[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}

	rows, err := s.db.Query(
		`SELECT p.crawl_id, p.url, COALESCE(p.title, '')
		 FROM pages_fts
		 JOIN pages p ON p.id = pages_fts.rowid
		 WHERE pages_fts MATCH ?
		 ORDER BY bm25(pages_fts)
		 LIMIT 50`, strings.Join(parts, " "))
	if err != nil {
		httpError(w, http.StatusInternalServerError, "db: "+err.Error())
		return
	}
	defer rows.Close()

	type hit struct {
		CrawlID int64  `json:"crawl_id"`
		URL     string `json:"url"`
		Title   string `json:"title"`
	}
	hits := []hit{}
	for rows.Next() {
		var h hit
		if err := rows.Scan(&h.CrawlID, &h.URL, &h.Title); err != nil {
			log.Printf("scan hit: %v", err) // тот же принцип, что в crawlPages
			continue
		}
		hits = append(hits, h)
	}
	if err := rows.Err(); err != nil {
		httpError(w, http.StatusInternalServerError, "rows: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"query": q, "hits": hits})
}

// --- утилиты ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}

func clampInt(s string, min, max, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func (s *Server) cancelCrawl(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		httpError(w, http.StatusBadRequest, "bad crawl id")
		return
	}
	// SETNX: повторный DELETE не продлевает и не путает статус.
	ok, err := s.rdb.SetNX(r.Context(), event.CancelKey(id), 1, 24*time.Hour).Result()
	if err != nil {
		httpError(w, http.StatusBadGateway, "redis: "+err.Error())
		return
	}
	status := "cancelled"
	if !ok {
		status = "already cancelling"
	}
	if _, err := s.db.Exec(`UPDATE crawls SET status = 'cancelled' WHERE id = ?`, id); err != nil {
		log.Printf("cancel status update: %v", err)
	}
	writeJSON(w, http.StatusOK, map[string]any{"crawl_id": id, "status": status})
}

func (s *Server) listCrawls(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(
		`SELECT id, url, depth, status, COALESCE(created_at, '')
		 FROM crawls ORDER BY id DESC LIMIT 50`)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "db: "+err.Error())
		return
	}
	defer rows.Close()

	type crawl struct {
		ID        int64  `json:"crawl_id"`
		URL       string `json:"url"`
		Depth     int    `json:"depth"`
		Status    string `json:"status"`
		CreatedAt string `json:"created_at"`
	}
	out := []crawl{}
	for rows.Next() {
		var c crawl
		if err := rows.Scan(&c.ID, &c.URL, &c.Depth, &c.Status, &c.CreatedAt); err != nil {
			log.Printf("scan crawl: %v", err)
			continue
		}
		// Живой статус поверх записанного: отмена важнее всего,
		// затем сверка счётчиков задач.
		switch {
		case s.rdb.Exists(r.Context(), event.CancelKey(c.ID)).Val() > 0:
			c.Status = "cancelled"
		default:
			pub, _ := s.rdb.Get(r.Context(), fmt.Sprintf("aranea:published:%d", c.ID)).Int()
			proc, _ := s.rdb.Get(r.Context(), fmt.Sprintf("aranea:processed:%d", c.ID)).Int()
			if pub > 0 && proc >= pub {
				c.Status = "finished"
			} else if c.Status == "" {
				c.Status = "running"
			}
		}
		out = append(out, c)
	}
	writeJSON(w, http.StatusOK, map[string]any{"crawls": out})
}

// debugPage показывает содержимое колонки text конкретной страницы —
// временный эндпоинт для диагностики конвейера полнотекстового поиска.
func (s *Server) debugPage(w http.ResponseWriter, r *http.Request) {
	crawlID, err := strconv.ParseInt(r.URL.Query().Get("crawl_id"), 10, 64)
	u := r.URL.Query().Get("url")
	if err != nil || u == "" {
		httpError(w, http.StatusBadRequest, "crawl_id and url are required")
		return
	}
	var title, text sql.NullString
	err = s.db.QueryRow(
		`SELECT COALESCE(title, ''), COALESCE(text, '')
		 FROM pages WHERE crawl_id = ? AND url = ?`, crawlID, u).
		Scan(&title, &text)
	if err == sql.ErrNoRows {
		httpError(w, http.StatusNotFound, "page not found")
		return
	}
	if err != nil {
		httpError(w, http.StatusInternalServerError, "db: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"crawl_id": crawlID, "url": u,
		"title": title.String, "text_len": len(text.String), "text": text.String,
	})
}
