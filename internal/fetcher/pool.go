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
			p.workerLoop(ctx, id) // TODO (вам) — см. ниже
		}(i)
	}

	// TODO: правильная последовательность завершения.
	// Пока ctx отменён:
	//  1) дождаться, пока воркеры дойдут до конца (wg.Wait) — они перестанут
	//     писать в p.results;
	//  2) ЗАТЕМ закрыть p.results — это разблокирует range у сборщика;
	//     закрыть results раньше = паника «send on closed channel»,
	//     если воркер ещё жив;
	//  3) дождаться сборщика (collectorDone).
}

// workerLoop — жизнь одного воркера.
// TODO: бесконечный select:
//   - case <-ctx.Done(): return  — краул остановлен;
//   - case t := <-p.tasks:      — взяли задачу, дальше:
//       1) p.limits.Wait(ctx, t.URL) — вежливость к домену (если ошибка — skip);
//       2) p.probe.Fetch(ctx, t) — сама проба;
//       3) положить результат в p.results.
// Нюанс: третья запись может блокироваться, если сборщик не успевает —
// оберните её в select с <-ctx.Done(), иначе воркер не выйдет при отмене.
