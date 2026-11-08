package fetcher

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"net/url"
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
	res := event.PageResult{CrawlID: t.CrawlID, URL: t.URL, Depth: t.Depth, MaxDepth: t.MaxDepth}

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
	// первые 512 КБ (заголовок и ссылки лежат в начале HTML).
	// Тело читаем один раз в память: оно нужно и для title, и для ссылок.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512<<10))
	if err != nil {
		// Ошибка чтения — валидный результат события, не падаем:
		// статус и latency уже зафиксированы, текст ошибки идёт в событие.
		res.Error = "read body: " + err.Error()
		return res
	}

	n := len(body)
	if n > 300 {
		n = 300
	}
	log.Printf("probe: %s body=%d snippet=%q", t.URL, len(body), string(body[:n]))

	// База для натягивания относительных ссылок — сам URL страницы.
	base, err := url.Parse(t.URL)
	if err != nil {
		res.Error = "bad url: " + err.Error()
		return res
	}

	res.Title = extractTitle(bytes.NewReader(body))
	res.Links = extractLinks(base, bytes.NewReader(body))
	return res
}

// extractLinks достаёт абсолютные http(s)-ссылки из HTML.
// Относительные ссылки натягиваются на базу (URL страницы), фрагменты
// (#...) отбрасываются — /page#a и /page для краула одно и то же.
func extractLinks(base *url.URL, body io.Reader) []string {
	seen := make(map[string]bool)
	var out []string

	tk := html.NewTokenizer(body)
	for {
		tt := tk.Next()
		if tt == html.ErrorToken {
			break // конец потока
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}
		name, hasAttr := tk.TagName()
		if string(name) != "a" || !hasAttr {
			continue
		}
		var href string
		for hasAttr {
			key, val, more := tk.TagAttr()
			if string(key) == "href" {
				href = string(val)
			}
			hasAttr = more
		}
		if href == "" {
			continue
		}
		u, err := base.Parse(href)
		if err != nil {
			continue
		}
		u.Fragment = ""
		if u.Scheme != "http" && u.Scheme != "https" {
			continue // mailto:, javascript: — не наше
		}
		if !seen[u.String()] {
			seen[u.String()] = true
			out = append(out, u.String())
		}
	}
	return out
}

// extractTitle достаёт текст первого <title>...</title> из HTML.
// Если заголовка нет (или документ не HTML) — возвращает пустую строку.
func extractTitle(body io.Reader) string {
	tk := html.NewTokenizer(body)
	inTitle := false
	for {
		tt := tk.Next()
		if tt == html.ErrorToken {
			return "" // конец потока — заголовок не встретился
		}
		switch tt {
		case html.StartTagToken:
			name, _ := tk.TagName()
			if string(name) == "title" {
				inTitle = true
			}
		case html.TextToken:
			if inTitle {
				return strings.TrimSpace(string(tk.Text()))
			}
		case html.EndTagToken:
			name, _ := tk.TagName()
			if string(name) == "title" {
				return "" // <title> закрылся пустым
			}
		}
	}
}
