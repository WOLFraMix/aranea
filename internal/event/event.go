// Package event — контракт событий в Kafka: топики и payloads.
package event

import (
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
	Topic    string `json:"topic"`
	Error    string `json:"error"`
	Attempts int    `json:"attempts"`
	Time     string `json:"time"`
	// Payload — оригинальное событие КАК СТРОКА: ядовитое письмо может
	// быть невалидным JSON, а конверт обязан всегда маршалиться.
	Payload string `json:"payload"`
}

// NewDLQMessage собирает конверт мёртвого письма — общая логика
// для consumer'а fetcher'а и router'а indexer'а.
func NewDLQMessage(topic string, payload []byte, attempts int, cause error) DLQMessage {
	return DLQMessage{
		Topic:    topic,
		Error:    cause.Error(),
		Attempts: attempts,
		Time:     time.Now().Format(time.RFC3339),
		Payload:  string(payload),
	}
}
