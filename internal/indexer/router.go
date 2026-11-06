package indexer

import (
	"context"
	"encoding/json"
	"log"
	"net/url"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/WOLFraMix/aranea/internal/config"
	"github.com/WOLFraMix/aranea/internal/event"
	"github.com/WOLFraMix/aranea/internal/fetcher"
)

// Router читает aranea.pages, сохраняет страницы, дедуплицирует ссылки
// и публикует новые задачи обратно в aranea.tasks. Это замыкает цикл.
type Router struct {
	cfg      config.Config
	store    *Store
	producer *fetcher.Producer // в фазе полировки вынесем в общий internal/kafka
	cl       *kgo.Client
}

func New(cfg config.Config, store *Store, prod *fetcher.Producer) (*Router, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaAddr),
		kgo.ConsumerGroup("aranea-indexers"),
		kgo.ConsumeTopics(event.TopicPages),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	return &Router{cfg: cfg, store: store, producer: prod, cl: cl}, nil
}

// Run — единственный цикл обработки: читает батч, обрабатывает, коммитит.
// Здесь НЕ нужны воркеры: узкое место — сеть и Redis, а не CPU,
// и дедуп в одном потоке безопасен по определению.
func (rt *Router) Run(ctx context.Context) {
	for {
		fetches := rt.cl.PollRecords(ctx, 100)
		if fetches.IsClientClosed() {
			return
		}
		if err := fetches.Err(); err != nil {
			log.Printf("consume pages: %v", err)
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			var res event.PageResult
			if err := json.Unmarshal(r.Value, &res); err != nil {
				log.Printf("bad page record: %v", err)
				return
			}
			rt.handle(ctx, res)
		})

		// Коммит ПОСЛЕ обработки: если упадём до коммита — сообщения
		// обработаются повторно после рестарта (at-least-once).
		// Повторная обработка безопасна: Save с INSERT OR IGNORE и
		// дедуп через SADD идемпотентны. Запомните эту пару — она ещё пригодится.
		rt.cl.CommitRecords(ctx, fetches.Records()...)
	}
}

// handle — вся бизнес-логика indexer'а для одной страницы.
func (rt *Router) handle(ctx context.Context, res event.PageResult) {
	if err := rt.store.Save(ctx, res); err != nil {
		log.Printf("save %s: %v", res.URL, err)
	}

	base, err := url.Parse(res.URL)
	if err != nil {
		return // база битая — ссылки не на что натягивать
	}

	for _, link := range res.Links {
		// ResolveReference: главное заклинание нормализации. Относительные
		// ссылки (/about, ../x, //other.com/a) правильно натягиваются на базу.
		u, err := base.Parse(link)
		if err != nil {
			continue
		}
		u.Fragment = "" // /page#a и /page — одна и та же страница для краула

		if u.Scheme != "http" && u.Scheme != "https" {
			continue // mailto:, javascript:, data: — не наше
		}
		if res.Depth+1 > rt.cfg.MaxDepth {
			continue
		}

		// Атомарный дедуп: false = кто-то уже видел, пропускаем молча.
		fresh, err := rt.store.Visit(ctx, u.String())
		if err != nil {
			log.Printf("dedup %s: %v", u, err)
			continue
		}
		if !fresh {
			continue
		}

		if err := rt.producer.Publish(ctx, event.TopicTasks, event.Task{
			CrawlID: res.CrawlID,
			URL:     u.String(),
			Depth:   res.Depth + 1,
		}); err != nil {
			log.Printf("publish task %s: %v", u, err)
		}
	}
}

func (rt *Router) Close() { rt.cl.Close() }
