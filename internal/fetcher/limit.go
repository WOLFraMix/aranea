// Package fetcher — конкурентное скачивание страниц.
package fetcher

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type limEntry struct {
	lim  *rate.Limiter
	last time.Time
}

// DomainLimits ограничивает частоту запросов отдельно на каждый домен.
// Это отличает вежливый краулер от «долбилки»: разные сайты не мешают
// друг другу, а один не нагружается чаще лимита.
type DomainLimits struct {
	mu   sync.Mutex
	lims map[string]*limEntry
	r    rate.Limit
	stop chan struct{}
}

// NewDomainLimits создаёт таблицу лимитов: perSec запросов/сек на домен.
// ctx нужен для корректного завершения janitor-горутины при shutdown.
func NewDomainLimits(ctx context.Context, perSec int) *DomainLimits {
	d := &DomainLimits{
		lims: make(map[string]*limEntry),
		r:    rate.Limit(perSec),
		stop: make(chan struct{}),
	}
	go d.janitor(ctx)
	return d
}

// janitor выселяет лимитеры доменов, к которым не обращались дольше часа:
// тысячи пройденных доменов не должны копиться в памяти вечно.
func (d *DomainLimits) janitor(ctx context.Context) {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-t.C:
			d.mu.Lock()
			for host, e := range d.lims {
				if time.Since(e.last) > time.Hour {
					delete(d.lims, host)
				}
			}
			d.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// Close останавливает janitor-горутину.
func (d *DomainLimits) Close() {
	select {
	case <-d.stop:
		// already closed
	default:
		close(d.stop)
	}
}

// Wait блокирует горутину, пока домен не разрешит следующий запрос,
// либо пока ctx не отменят. Возвращает nil, когда можно идти.
func (d *DomainLimits) Wait(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("cant parse URL: %w", err)
	}
	// Имя хоста не зависит от регистра: Example.com и example.com — один домен.
	// Без приведения к нижнему регистру им достались бы разные лимитеры,
	// и суммарная нагрузка на сайт удвоилась бы.
	host := strings.ToLower(u.Hostname())
	if host == "" {
		// Без хоста ключ был бы "", и все такие URL делили бы один общий лимитер.
		return fmt.Errorf("URL without host: %q", rawURL)
	}

	// get-or-create под ОДНИМ захватом мьютекса: проверка и вставка в map
	// атомарны, поэтому два воркера не создадут по лимитеру на один домен.
	d.mu.Lock()
	entry, ok := d.lims[host]
	if !ok {
		entry = &limEntry{lim: rate.NewLimiter(d.r, 1)}
		d.lims[host] = entry
	}
	entry.last = time.Now()
	limiter := entry.lim
	d.mu.Unlock()

	// Ждём уже ВНЕ мьютекса: rate.Limiter безопасен для параллельного использования,
	// а мьютекс, который держался бы во время ожидания, заставил бы все домены
	// ждать друг друга.
	if err := limiter.Wait(ctx); err != nil {
		return fmt.Errorf("wait for %s: %w", host, err)
	}
	return nil
}
