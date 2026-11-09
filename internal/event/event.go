// Package event — контракт событий в Kafka: топики и payloads.
package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// TopicTasks — очередь URL: api кладёт seed, indexer кладёт найденные ссылки.
const TopicTasks = "aranea.tasks"

// TopicPages — результаты: fetcher публикует, indexer потребляет.
const TopicPages = "aranea.pages"

// SeenTTL — срок жизни дедуп-SET краула. Обновляется каждым SAdd,
// поэтому отсчёт идёт от последней активности краула.
const SeenTTL = 24 * time.Hour

// Task — единица работы для fetcher: «скачай этот URL».
type Task struct {
	CrawlID  int64  `json:"crawl_id"`
	URL      string `json:"url"`
	Depth    int    `json:"depth"`
	MaxDepth *int   `json:"max_depth,omitempty"` // лимит ЭТОГО краула
}

// PageResult — то, что вернул fetcher после скачивания.
// Частично повторяет domain.Page — но это ОСОЗНАННОЕ дублирование:
// Kafka-контракт не должен зависеть от внутренней модели хранилища,
// иначе менять одно придётся через другое.
type PageResult struct {
	CrawlID    int64    `json:"crawl_id"`
	URL        string   `json:"url"`
	Depth      int      `json:"depth"`
	StatusCode int      `json:"status_code"`
	LatencyMs  int64    `json:"latency_ms"`
	Title      string   `json:"title,omitempty"`
	Text       string   `json:"text,omitempty"`
	Error      string   `json:"error,omitempty"`
	Links      []string `json:"links,omitempty"`     // все href со страницы, до нормализации
	MaxDepth   *int     `json:"max_depth,omitempty"` // лимит ЭТОГО краула
}

// CancelKey — маркер отмены краула: SETNX от API, EXISTS в indexer'е.
func CancelKey(crawlID int64) string {
	return fmt.Sprintf("aranea:cancel:%d", crawlID)
}

// TopicDLQ — топик «мёртвых писем»: события, которые потребитель
// не смог обработать после исчерпания попыток.
const TopicDLQ = "aranea.dlq"

// DLQMessage — конверт мёртвого письма: оригинальное событие целиком,
// причина отказа и число попыток. Достаточно для разбора и переигрывания.
type DLQMessage struct {
	Topic    string          `json:"topic"`
	Error    string          `json:"error"`
	Attempts int             `json:"attempts"`
	Time     string          `json:"time"`
	Payload  json.RawMessage `json:"payload"`
}
