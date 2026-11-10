package fetcher

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
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

// NewProbe — клиент с жёстким таймаутом из конфига и защитой от SSRF.
func NewProbe(timeout time.Duration) *Probe {
	// Кастомный Transport без Proxy: запросы не идут через системный
	// прокси, что заодно сужает SSRF-поверхность.
	return &Probe{client: &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: restrictedDialer(timeout),
		},
	}}
}

// restrictedDialer возвращает DialContext, который блокирует подключения
// к приватным и локальным адресам (loopback, link-local, RFC1918, Docker/MetaDNS).
// Защищает от SSRF, когда пользовательский URL попадает в краулер.
func restrictedDialer(timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if isPrivateIP(host) {
			return nil, fmt.Errorf("SSRF blocked: %s is a private IP", host)
		}
		// Не IP-адрес — резолвим и проверяем. DefaultResolver.LookupHost
		// принимает ctx: при отмене запроса горутина не виснет в DNS.
		addrs, err := net.DefaultResolver.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, a := range addrs {
			if isPrivateIP(a) {
				return nil, fmt.Errorf("SSRF blocked: %s resolves to private IP", host)
			}
		}
		d := &net.Dialer{Timeout: timeout}
		return d.DialContext(ctx, network, addr)
	}
}

// isPrivateIP возвращает true, если адрес попадает в приватные диапазоны.
func isPrivateIP(s string) bool {
	ip := net.ParseIP(s)
	if ip == nil {
		return false
	}
	// IsPrivate покрывает RFC1918 и IPv6 ULA (fc00::/7), IsUnspecified —
	// 0.0.0.0/::. CGNAT 100.64/10 IsPrivate не считает приватным,
	// но для SSRF-защиты его блокируем отдельной проверкой.
	if ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		return v4[0] == 100 && v4[1] >= 64 && v4[1] < 128 // 100.64.0.0/10
	}
	return false
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

	// База для натягивания относительных ссылок — сам URL страницы.
	base, err := url.Parse(t.URL)
	if err != nil {
		res.Error = "bad url: " + err.Error()
		return res
	}

	res.Title = extractTitle(bytes.NewReader(body))
	res.Text = extractDescription(bytes.NewReader(body))
	if res.Text == "" {
		res.Text = extractBodyText(bytes.NewReader(body))
	}
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

// extractBodyText собирает текст всех <p>-абзацев страницы до указанного лимита.
// Для карточек товаров это описание и метаинформация (автор, цена) —
// как раз то, что нужно для полнотекстового поиска.
func extractBodyText(body io.Reader) string {
	tk := html.NewTokenizer(body)
	var inP bool
	var buf []string
	total := 0
	for {
		tt := tk.Next()
		if tt == html.ErrorToken || total >= 2000 {
			break // конец потока или набрали достаточно
		}
		switch tt {
		case html.StartTagToken:
			name, _ := tk.TagName()
			if string(name) == "p" {
				inP = true
			}
		case html.TextToken:
			if inP {
				s := strings.TrimSpace(string(tk.Text()))
				if s != "" {
					buf = append(buf, s)
					total += len(s)
				}
			}
		case html.EndTagToken:
			name, _ := tk.TagName()
			if string(name) == "p" {
				inP = false
			}
		}
	}
	return strings.Join(buf, " ")
}

// extractDescription достаёт content атрибут тега
// <meta name="description" content="..."> — компактное осмысленное
// описание страницы: на карточках товаров там лежит их текст.
func extractDescription(body io.Reader) string {
	tk := html.NewTokenizer(body)
	for {
		tt := tk.Next()
		if tt == html.ErrorToken {
			return ""
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}
		name, hasAttr := tk.TagName()
		if string(name) != "meta" || !hasAttr {
			continue
		}
		var isDesc bool
		var content string
		for hasAttr {
			key, val, more := tk.TagAttr()
			switch string(key) {
			case "name":
				if strings.EqualFold(string(val), "description") {
					isDesc = true
				}
			case "content":
				content = string(val)
			}
			hasAttr = more
		}
		if isDesc {
			return strings.TrimSpace(content)
		}
	}
}
