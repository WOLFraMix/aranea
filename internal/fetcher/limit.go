// Package fetcher — конкурентное скачивание страниц.
package fetcher

import (
	"context"
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
	// TODO три шага:
	// 1) распарсить URL (net/url.Parse) и взять u.Hostname() — это ключ домена;
	//    при ошибке парсинга вернуть ошибку — на такой URL идти нельзя;
	// 2) под мьютексом: получить limiter из map; если его нет — создать
	//    rate.NewLimiter(d.r, 1) и положить в map (burst=1: не копим «пакеты» запросов);
	// 3) вернуть limiter.Wait(ctx).
	//
	// Подсказка по идиоме get-or-create: проверка и вставка в map должны
	// происходить под ОДНИМ захватом мьютекса — иначе два воркера могут
	// одновременно не найти limiter и создать по своему.
	return nil
}
