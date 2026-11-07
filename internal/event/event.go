// Package event — контракт событий в Kafka: топики и payloads.
package event

import "fmt"

// TopicTasks — очередь URL: api кладёт seed, indexer кладёт найденные ссылки.
const TopicTasks = "aranea.tasks"

// TopicPages — результаты: fetcher публикует, indexer потребляет.
const TopicPages = "aranea.pages"

// Task — единица работы для fetcher: «скачай этот URL».
type Task struct {
	CrawlID int64  `json:"crawl_id"`
	URL     string `json:"url"`
	Depth   int    `json:"depth"` // глубина от seed: 0 у самого seed
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
	Error      string   `json:"error,omitempty"`
	Links      []string `json:"links,omitempty"` // все href со страницы, до нормализации
}

// CancelKey — маркер отмены краула: SETNX от API, EXISTS в indexer'е.
func CancelKey(crawlID int64) string {
	return fmt.Sprintf("aranea:cancel:%d", crawlID)
}
