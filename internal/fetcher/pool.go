package fetcher

import (
	"context"
	"log"
	"sync"

	"github.com/WOLFraMix/aranea/internal/config"
	"github.com/WOLFraMix/aranea/internal/event"
	"github.com/redis/go-redis/v9"
)

// Pool — пул воркеров: fan-out на пробы, fan-in на сборщик результатов.
type Pool struct {
	cfg      config.Config
	probe    *Probe
	limits   *DomainLimits
	producer *Producer
	robots   *Robots
	rdb      *redis.Client // проверка отмены краула перед скачиванием

	tasks   chan event.Task
	results chan event.PageResult
}

func New(cfg config.Config, pr *Probe, lim *DomainLimits, prod *Producer, rb *Robots, rdb *redis.Client) *Pool {
	return &Pool{
		cfg:      cfg,
		probe:    pr,
		limits:   lim,
		producer: prod,
		robots:   rb,
		rdb:      rdb,
		tasks:    make(chan event.Task, cfg.Workers*2),
		results:  make(chan event.PageResult, cfg.Workers*2),
	}
}

// Tasks даёт консюмеру канал, куда класть задачи.
func (p *Pool) Tasks() chan<- event.Task { return p.tasks }

// Run запускает сборщик и воркеров; блокируется до отмены ctx.
func (p *Pool) Run(ctx context.Context) {
	// Контекст зачистки: наследует значения родителя, но НЕ отменяется вместе
	// с ним. Зачем: когда ctx отменён (Ctrl+C), воркеры выходят, а сборщик
	// ещё должен дописать хвост результатов из буфера канала. С обычным ctx
	// каждый Publish падал бы с "context canceled" — и результаты терялись.
	pctx := context.WithoutCancel(ctx)

	// Сборщик: единственная горутина, которая трогает Kafka-продюсер
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for r := range p.results {
			if err := p.producer.Publish(pctx, event.TopicPages, r); err != nil {
				log.Printf("publish page: %v", err)
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.workerLoop(ctx, id)
		}(i)
	}

	// Завершение: Run блокируется до отмены ctx, затем гасит пул в строгом порядке.
	<-ctx.Done()

	// 1) Ждём, пока все воркеры выйдут: после этого никто не пишет в p.results.
	wg.Wait()
	// 2) Только теперь закрываем results — range у сборщика завершится.
	//    Раньше было бы «send on closed channel», если воркер ещё жив.
	close(p.results)
	// 3) Ждём выхода сборщика.
	<-collectorDone
}

// workerLoop — жизнь одного воркера: берёт задачу, ждёт вежливости домена,
// пробирает страницу и отдаёт результат сборщику. Выходит при отмене ctx.
func (p *Pool) workerLoop(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-p.tasks:
			// Отменённый краул: задачу выбрасываем без скачивания.
			// Проверка стоит до всего — ни вежливости, ни robots, ни fetch.
			if p.cancelled(ctx, t.CrawlID) {
				log.Printf("worker: dropped task %s (crawl %d cancelled)", t.URL, t.CrawlID)
				continue
			}
			// Вежливость к домену. Ошибка здесь — битый URL или отмена ctx.
			if err := p.limits.Wait(ctx, t.URL); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("worker %d: skip %s: %v", id, t.URL, err)
				continue
			}
			if !p.robots.Allow(ctx, t.URL) {
				// Блокировка robots — тоже результат: краул увидит её в базе,
				// а счётчик processed не зависнет.
				res := event.PageResult{
					CrawlID: t.CrawlID, URL: t.URL, Depth: t.Depth,
					MaxDepth: t.MaxDepth, Error: "robots disallow",
				}
				select {
				case p.results <- res:
				case <-ctx.Done():
					return
				}
				continue
			}

			res := p.probe.Fetch(ctx, t)

			// Запись может блокироваться, если сборщик не успевает.
			// Без select с ctx.Done воркер повис бы и не вышел при отмене.
			select {
			case p.results <- res:
			case <-ctx.Done():
				return
			}
			log.Printf("worker: published page %s links=%d", res.URL, len(res.Links))
		}
	}
}

// cancelled спрашивает у Redis, не отменён ли краул.
// При ошибке Redis отвечаем «не отменён»: сбой кэша не должен глушить конвейер.
func (p *Pool) cancelled(ctx context.Context, crawlID int64) bool {
	n, err := p.rdb.Exists(ctx, event.CancelKey(crawlID)).Result()
	if err != nil {
		log.Printf("worker: cancel check failed: %v", err)
		return false
	}
	return n > 0
}
