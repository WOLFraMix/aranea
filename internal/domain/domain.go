// Package domain — модели данных, общие для всех трёх сервисов.
package domain

// Crawl — один запуск обхода: от seed URL до исчерпания лимитов.
type Crawl struct {
	ID       int64  `json:"id"`
	SeedURL  string `json:"seed_url"`
	MaxDepth int    `json:"max_depth"`
	// Status: queued | running | done | failed
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

// Page — результат скачивания одной страницы.
type Page struct {
	CrawlID    int64  `json:"crawl_id"`
	URL        string `json:"url"`
	Depth      int    `json:"depth"`
	StatusCode int    `json:"status_code"`
	LatencyMs  int64  `json:"latency_ms"`
	Title      string `json:"title,omitempty"`
	Error      string `json:"error,omitempty"` // сетевая ошибка или запрет robots.txt
}

// Link — направленное ребро графа: со страницы на страницу.
type Link struct {
	CrawlID int64  `json:"crawl_id"`
	FromURL string `json:"from_url"`
	ToURL   string `json:"to_url"`
	// Internal — ссылка на тот же хост, что у seed: только они продлевают обход
	Internal bool `json:"internal"`
}
