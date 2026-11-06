package fetcher

import (
	"context"
	"encoding/json"
	"log"

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
		// Новая группа: читать с начала топика, а не с «сейчас».
		// Без этого задачу, подложенную до старта fetcher'а, легко пропустить.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl}, nil
}

// ConsumeTasks перекладывает задачи из Kafka в канал пула.
// Блокируется до отмены ctx; закрываемый канал done позволяет main
// дождаться завершения — та самая «подшивка горутины» из разбора main.go.
func (c *Consumer) ConsumeTasks(ctx context.Context, tasks chan<- event.Task) (chan struct{}, error) {
	done := make(chan struct{})

	// Блокировка на старте: если брокер ещё не готов (compose греется),
	// PollRecords будет молча возвращать пусто — это не страшно, цикл
	// просто крутится. Но группу надо зарегистрировать заранее, поэтому
	// здесь ничего дополнительно не ждём.

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
					// Битое сообщение не останавливает весь поток:
					// логируем и забываем. В фазе 3 обсудим dead-letter.
					log.Printf("consume: bad task record: %v", err)
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
