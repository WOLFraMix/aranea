package fetcher

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/WOLFraMix/aranea/internal/event"
)

// Consumer — читает задачи из топика aranea.tasks.
type Consumer struct {
	cl *kgo.Client
}

// NewConsumer подключается к брокеру как участник группы aranea-fetchers.
func NewConsumer(addr string) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup("aranea-fetchers"),
		kgo.ConsumeTopics(event.TopicTasks),
		// Офсеты коммитим после обработки (at-least-once): рестарт
		// дообслуживает недообработанное, а не переигрывает всю историю.
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl}, nil
}

// toDLQ отправляет «мёртвое письмо» в aranea.dlq. Битый JSON ретраить
// бессмысленно — он не починится сам. Если и DLQ-публикация не удалась,
// логируем потерю: поток обработки не должен застревать на ядовитом письме.
func (c *Consumer) toDLQ(ctx context.Context, payload []byte, cause error) {
	msg := event.DLQMessage{
		Topic:    event.TopicTasks,
		Error:    cause.Error(),
		Attempts: 1,
		Time:     time.Now().Format(time.RFC3339),
		Payload:  payload,
	}
	val, err := json.Marshal(msg)
	if err != nil {
		log.Printf("dlq marshal: %v", err)
		return
	}
	c.cl.Produce(ctx, &kgo.Record{Topic: event.TopicDLQ, Value: val},
		func(_ *kgo.Record, err error) {
			if err != nil {
				log.Printf("dlq produce (оригинал потерян): %v", err)
				return
			}
			log.Printf("dlq: poison record отправлен в %s: %v", event.TopicTasks, cause)
		})
}

// ConsumeTasks перекладывает задачи из Kafka в канал пула.
// Блокируется до отмены ctx; закрываемый канал done позволяет main
// дождаться завершения — та самая «подшивка горутины» из разбора main.go.
func (c *Consumer) ConsumeTasks(ctx context.Context, tasks chan<- event.Task) (chan struct{}, error) {
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			// PollRecords забирает до 100 записей или возвращает сразу,
			// если ctx отменён. Больше 100 не нужно: канал пула буферизован
			// на 2×воркеров, всё равно упрёмся в него раньше.
			fetches := c.cl.PollRecords(ctx, 100)
			if fetches.IsClientClosed() {
				return // клиент закрыли (defer Close в main) — выходим
			}
			if err := fetches.Err(); err != nil {
				if ctx.Err() != nil {
					return // контекст отменён — это сигнал остановки, а не сбой чтения
				}
				// Ошибка чтения — не повод умирать: брокер мог мигнуть.
				log.Printf("consume: %v", err)
				continue
			}

			fetches.EachRecord(func(r *kgo.Record) {
				var t event.Task
				if err := json.Unmarshal(r.Value, &t); err != nil {
					// Ядовитое письмо — в DLQ, оффсет всё равно закоммитится
					// вместе с батчем: поток продолжается.
					c.toDLQ(ctx, r.Value, err)
					return
				}
				select {
				case tasks <- t:
					// Канал буферизован, но при заторе пул может притормаживать —
					// тогда каждая горутина-читатель встанет здесь, и это ок:
					// консюмер один, он просто накопит следующий батч.
				case <-ctx.Done():
					return
				}
				log.Printf("consume: got task %s", t.URL)
			})
		}
	}()

	return done, nil
}

func (c *Consumer) Close() { c.cl.Close() }
