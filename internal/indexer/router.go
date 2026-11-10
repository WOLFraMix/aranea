package indexer

import (
	"context"
	"encoding/json"
	"log"
	"net/url"
	"time"

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
			if ctx.Err() != nil {
				return // отмена = сигнал остановки, а не сбой чтения
			}
			log.Printf("consume pages: %v", err)
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			var res event.PageResult
			if err := json.Unmarshal(r.Value, &res); err != nil {
				// Битое событие чинить нечем — сразу в DLQ.
				rt.toDLQ(ctx, event.TopicPages, r.Value, 1, err)
				return
			}
			rt.saveWithRetry(ctx, res, r.Value)

			base, err := url.Parse(res.URL)
			if err != nil {
				return // база битая — ссылки не на что натягивать
			}

			// Лимит краула: nil = событие старого формата, берём глобальный конфиг.
			// Именно nil, а не ноль: depth 0 — валидный краул «только seed».
			limit := rt.cfg.MaxDepth
			if res.MaxDepth != nil {
				limit = *res.MaxDepth
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
				if res.Depth+1 > limit {
					continue
				}

				// Атомарный дедуп: false = кто-то уже видел, пропускаем молча.
				fresh, err := rt.store.Visit(ctx, res.CrawlID, u.String())
				if err != nil {
					log.Printf("dedup %s: %v", u, err)
					continue
				}
				if !fresh {
					continue
				}

				if err := rt.store.CountPublished(ctx, res.CrawlID); err != nil {
					log.Printf("count published: %v", err)
				}

				if err := rt.producer.Publish(ctx, event.TopicTasks, event.Task{
					CrawlID:  res.CrawlID,
					URL:      u.String(),
					Depth:    res.Depth + 1,
					MaxDepth: res.MaxDepth,
				}); err != nil {
					log.Printf("publish task %s: %v", u, err)
				}
			}
		})

		// Коммит ПОСЛЕ обработки: если упадём до коммита — сообщения
		// обработаются повторно после рестарта (at-least-once).
		// Повторная обработка безопасна: Save с INSERT OR IGNORE и
		// дедуп через SADD идемпотентны. Запомните эту пару — она ещё пригодится.
		rt.cl.CommitRecords(ctx, fetches.Records()...)
	}
}

// saveWithRetry повторяет Save с линейным бэкоффом; после исчерпания
// попыток отправляет событие в DLQ — ядовитое письмо не задерживает
// партицию. Повторный Save безопасен: INSERT OR IGNORE идемпотентен.
func (rt *Router) saveWithRetry(ctx context.Context, res event.PageResult, raw []byte) {
	const maxAttempts = 3
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = rt.store.Save(ctx, res); err == nil {
			return
		}
		log.Printf("save %s: попытка %d/%d: %v", res.URL, attempt, maxAttempts, err)
		select {
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		case <-ctx.Done():
			return // остановка важнее ретраев
		}
	}
	rt.toDLQ(ctx, event.TopicPages, raw, maxAttempts, err)
}

// toDLQ публикует «мёртвое письмо» — оригинальное событие с причиной.
func (rt *Router) toDLQ(ctx context.Context, topic string, payload []byte, attempts int, cause error) {
	msg := event.NewDLQMessage(topic, payload, attempts, cause)
	if err := rt.producer.Publish(ctx, event.TopicDLQ, msg); err != nil {
		log.Printf("dlq publish (оригинал потерян): %v", err)
		return
	}
	log.Printf("dlq: письмо отправлено из %s: %v", topic, cause)
}

func (rt *Router) Close() { rt.cl.Close() }
