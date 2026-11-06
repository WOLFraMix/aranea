package fetcher

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Robots кеширует robots.txt по хосту и проверяет разрешение на путь.
type Robots struct {
	client *http.Client
	mu     sync.Mutex
	rules  map[string]*ruleSet // host -> правила
}

type ruleSet struct{ disallow []string }

func NewRobots(timeout time.Duration) *Robots {
	return &Robots{
		client: &http.Client{Timeout: timeout},
		rules:  make(map[string]*ruleSet),
	}
}

// Allow отвечает, можно ли запрашивать путь. Ошибка загрузки robots.txt
// трактуется как разрешение — так делает большинство краулеров.
func (r *Robots) Allow(ctx context.Context, rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return false
	}
	host := u.Hostname()

	rs := r.load(ctx, host)
	path := u.Path
	if path == "" {
		path = "/"
	}
	for _, prefix := range rs.disallow {
		if strings.HasPrefix(path, prefix) {
			return false
		}
	}
	return true
}

func (r *Robots) load(ctx context.Context, host string) *ruleSet {
	r.mu.Lock()
	if rs, ok := r.rules[host]; ok {
		r.mu.Unlock()
		return rs
	}
	r.mu.Unlock()

	rs := &ruleSet{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://"+host+"/robots.txt", nil)
	if err == nil {
		req.Header.Set("User-Agent", "aranea/1.0")
		if resp, err := r.client.Do(req); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				rs = parseRobots(resp.Body)
			}
		}
	}

	r.mu.Lock()
	r.rules[host] = rs
	r.mu.Unlock()
	return rs
}

// parseRobots — минимальный разбор: секция User-agent: *, строки Disallow.
func parseRobots(body io.Reader) *ruleSet {
	rs := &ruleSet{}
	inStar := false
	sc := bufio.NewScanner(body)
	for sc.Scan() {
		line := strings.TrimSpace(strings.SplitN(sc.Text(), "#", 2)[0])
		key, val, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		key, val = strings.ToLower(strings.TrimSpace(key)), strings.TrimSpace(val)
		switch key {
		case "user-agent":
			inStar = val == "*"
		case "disallow":
			if inStar && val != "" {
				rs.disallow = append(rs.disallow, val)
			}
		}
	}
	return rs
}
