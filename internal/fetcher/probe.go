package fetcher

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/html"

	"github.com/WOLFraMix/aranea/internal/event"
)

// Probe — один HTTP-клиент на весь fetcher: переиспользует соединения.
type Probe struct {
	client *http.Client
}

// NewProbe — клиент с жёстким таймаутом из конфига.
func NewProbe(timeout time.Duration) *Probe {
	return &Probe{client: &http.Client{Timeout: timeout}}
}

// Fetch скачивает страницу и возвращает результат пробы.
// Сетевые ошибки НЕ паникуем и НЕ ретраим здесь — неудача это валидный
// результат события, indexer решит, что с ней делать.
func (pr *Probe) Fetch(ctx context.Context, t event.Task) event.PageResult {
	started := time.Now()
	res := event.PageResult{CrawlID: t.CrawlID, URL: t.URL, Depth: t.Depth}

	// ctx в запросе: отмена краула отменяет и летящие HTTP-запросы
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		res.Error = "bad url: " + err.Error()
		return res
	}
	// Честный User-Agent: краулер представляется собой, а не маскируется под браузер
	req.Header.Set("User-Agent", "aranea/1.0 (+https://github.com/WOLFraMix/aranea)")

	resp, err := pr.client.Do(req)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	defer resp.Body.Close()

	res.StatusCode = resp.StatusCode
	res.LatencyMs = time.Since(started).Milliseconds()

	// Ограничение чтения: страница может быть на гигабайт, нам нужны только
	// первые 512 КБ (заголовок и ссылки лежат в начале HTML)
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		res.Error = err.Error()
		return res
	}

	res.Title, res.Links = extractLinks(body)
	return res
}

// extractLinks разбирает HTML и достаёт заголовок и все href.
func extractLinks(body []byte) (title string, links []string) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return "", nil // битый HTML — не краулера проблема, отдаём пустоту
	}
	// Рекурсивный обход дерева узлов: у каждого элемента <a> берём href,
	// у <title> — текст. Это стандартная идиома для x/net/html.
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, a := range n.Attr {
				if a.Key == "href" && a.Val != "" {
					links = append(links, a.Val)
				}
			}
		}
		if n.Type == html.ElementNode && n.Data == "title" && n.FirstChild != nil {
			title = strings.TrimSpace(n.FirstChild.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return title, links
}
