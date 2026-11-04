// Package fetcher — конкурентное скачивание страниц.
package fetcher

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"golang.org/x/time/rate"
)

// DomainLimits ограничивает частоту запросов отдельно на каждый домен.
// Это отличает вежливый краулер от «долбилки»: разные сайты не мешают
// друг другу, а один не нагружается чаще лимита.
type DomainLimits struct {
	mu   sync.Mutex               // защищает доступ к map из многих воркеров
	lims map[string]*rate.Limiter // host -> лимитер этого домена
	r    rate.Limit               // запросов в секунду на каждый домен
}

// NewDomainLimits создаёт таблицу лимитов: perSec запросов/сек на домен.
func NewDomainLimits(perSec int) *DomainLimits {
	return &DomainLimits{
		lims: make(map[string]*rate.Limiter),
		r:    rate.Limit(perSec),
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
	limiter, ok := d.lims[host]
	if !ok {
		limiter = rate.NewLimiter(d.r, 1) // burst=1: не копим «пакеты» запросов
		d.lims[host] = limiter
	}
	d.mu.Unlock()

	// Ждём уже ВНЕ мьютекса: rate.Limiter безопасен для параллельного использования,
	// а мьютекс, который держался бы во время ожидания, заставил бы все домены
	// ждать друг друга.
	if err := limiter.Wait(ctx); err != nil {
		return fmt.Errorf("wait for %s: %w", host, err)
	}
	return nil
}
