package fetcher

import (
	"context"
	"encoding/json"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer — обёртка над franz-go: одна точка публикации событий.
type Producer struct {
	cl *kgo.Client
}

// NewProducer подключается к брокеру и ждёт его готовности.
func NewProducer(addr string) (*Producer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		return nil, err
	}
	// Ping: дождаться, пока брокер реально примет соединение.
	// Без этого первый Produce может упасть, если compose ещё греется.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, err
	}
	return &Producer{cl: cl}, nil
}

// Publish сериализует событие и синхронно ждёт подтверждения брокера.
// ProduceSync: простая семантика «записали или ошибка» — асинхронные
// батчи добавим, если упрёмся в производительность (не упрёмся).
func (p *Producer) Publish(ctx context.Context, topic string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: b}).FirstErr()
}

func (p *Producer) Close() { p.cl.Close() }
