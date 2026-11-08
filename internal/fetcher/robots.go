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

// rule — одна директива Allow/Disallow внутри группы.
type rule struct {
	prefix string
	allow  bool
}

// ruleSet — правила одного хоста и время загрузки (для выселения из кеша).
type ruleSet struct {
	rules    []rule
	loadedAt time.Time
}

// Robots кеширует robots.txt по хосту и проверяет разрешение на путь.
type Robots struct {
	client *http.Client
	mu     sync.Mutex
	rules  map[string]*ruleSet
	ttl    time.Duration
	ua     string // база юзер-агента без версии: "aranea"
}

func NewRobots(timeout time.Duration) *Robots {
	r := &Robots{
		client: &http.Client{Timeout: timeout},
		rules:  make(map[string]*ruleSet),
		ttl:    time.Hour,
		ua:     "aranea",
	}
	go r.janitor()
	return r
}

// janitor раз в 10 минут выселяет остывшие записи: карта не растёт бесконечно.
// Живёт столько же, сколько процесс, — отдельного shutdown не требует.
func (r *Robots) janitor() {
	t := time.NewTicker(10 * time.Minute)
	for range t.C {
		r.mu.Lock()
		for host, rs := range r.rules {
			if time.Since(rs.loadedAt) > r.ttl {
				delete(r.rules, host)
			}
		}
		r.mu.Unlock()
	}
}

// Allow отвечает, можно ли запрашивать путь. Ошибка загрузки robots.txt
// трактуется как разрешение — так делает большинство краулеров.
func (r *Robots) Allow(ctx context.Context, rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return false
	}

	rs := r.load(ctx, u.Hostname())
	path := u.Path
	if path == "" {
		path = "/"
	}

	// Побеждает правило с САМЫМ ДЛИННЫМ совпавшим префиксом;
	// при равной длине — Allow. Это даёт пары «Disallow: /» + «Allow: /public/».
	best := -1
	allowed := true
	for i, ru := range rs.rules {
		if !strings.HasPrefix(path, ru.prefix) {
			continue
		}
		if best == -1 ||
			len(ru.prefix) > len(rs.rules[best].prefix) ||
			(len(ru.prefix) == len(rs.rules[best].prefix) && ru.allow) {
			best = i
			allowed = ru.allow
		}
	}
	return allowed
}

func (r *Robots) load(ctx context.Context, host string) *ruleSet {
	r.mu.Lock()
	rs, ok := r.rules[host]
	fresh := ok && time.Since(rs.loadedAt) < r.ttl
	r.mu.Unlock()
	if fresh {
		return rs
	}

	// Свежих нет — загружаем заново. Параллельная загрузка двумя горутинами
	// безвредна: результат идемпотентен, перезапись в кеш атомарна.
	loaded := &ruleSet{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://"+host+"/robots.txt", nil)
	if err == nil {
		req.Header.Set("User-Agent", r.ua+"/1.0")
		if resp, err := r.client.Do(req); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				loaded = parseRobots(resp.Body, r.ua)
			}
		}
	}
	loaded.loadedAt = time.Now()

	r.mu.Lock()
	r.rules[host] = loaded
	r.mu.Unlock()
	return loaded
}

// parseRobots разбирает robots.txt по RFC 9309: строки подряд User-agent
// образуют одну группу, за ней идут её Allow/Disallow.
func parseRobots(body io.Reader, ua string) *ruleSet {
	type group struct {
		agents []string
		rules  []rule
	}
	var groups []group
	var cur *group

	sc := bufio.NewScanner(body)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		key, val, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		val = strings.TrimSpace(val)

		switch key {
		case "user-agent":
			// Несколько User-agent подряд — одна группа; правила уже
			// появились — начинается новая.
			if cur == nil || len(cur.rules) > 0 {
				groups = append(groups, group{})
				cur = &groups[len(groups)-1]
			}
			cur.agents = append(cur.agents, strings.ToLower(val))
		case "allow", "disallow":
			if cur == nil || val == "" {
				continue // пустой Disallow = «разрешено всё»
			}
			cur.rules = append(cur.rules, rule{prefix: val, allow: key == "allow"})
		}
	}

	// Выбор группы: длиннейший совпавший агент. «aranea» матчится и с
	// секцией «aranea/1.0», и наоборот — сравнение в обе стороны.
	best, bestLen := -1, -1
	for i, g := range groups {
		for _, a := range g.agents {
			if strings.HasPrefix(a, ua) || strings.HasPrefix(ua, a) && len(a) > bestLen {
				best, bestLen = i, len(a)
			}
		}
	}
	if best == -1 {
		// Совпадений нет — по RFC берём группу *, она всегда в конце файла.
		for i := len(groups) - 1; i >= 0; i-- {
			for _, a := range groups[i].agents {
				if a == "*" {
					best = i
					break
				}
			}
			if best >= 0 {
				break
			}
		}
	}
	if best == -1 {
		return &ruleSet{} // валидных групп нет — всё разрешено
	}
	return &ruleSet{rules: groups[best].rules}
}
