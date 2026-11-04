package fetcher

import (
	"context"
	"log"
	"sync"

	"github.com/WOLFraMix/aranea/internal/config"
	"github.com/WOLFraMix/aranea/internal/event"
)

// Pool — пул воркеров: fan-out на пробы, fan-in на сборщик результатов.
type Pool struct {
	cfg      config.Config
	probe    *Probe
	limits   *DomainLimits
	producer *Producer

	tasks   chan event.Task       // задачи воркерам
	results chan event.PageResult // результаты от воркеров — одному сборщику
}

func New(cfg config.Config, pr *Probe, lim *DomainLimits, prod *Producer) *Pool {
	// Буферы = 2×воркеров: воркеры не блокируются друг о друга,
	// а консюмер и продюсер не гасят пропускную способность пула
	return &Pool{
		cfg:      cfg,
		probe:    pr,
		limits:   lim,
		producer: prod,
		tasks:    make(chan event.Task, cfg.Workers*2),
		results:  make(chan event.PageResult, cfg.Workers*2),
	}
}

// Tasks даёт консюмеру канал, куда класть задачи.
func (p *Pool) Tasks() chan<- event.Task { return p.tasks }

// Run запускает сборщик и воркеров; блокируется до отмены ctx.
func (p *Pool) Run(ctx context.Context) {
	// Сборщик: единственная горутина, которая трогает Kafka-продюсер
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for r := range p.results {
			if err := p.producer.Publish(ctx, event.TopicPages, r); err != nil {
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
			// Вежливость к домену. Ошибка здесь — битый URL или отмена ctx.
			if err := p.limits.Wait(ctx, t.URL); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("worker %d: skip %s: %v", id, t.URL, err)
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
		}
	}
}
