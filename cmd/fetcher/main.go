// fetcher: качает страницы из очереди задач и публикует результаты.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/WOLFraMix/aranea/internal/config"
	"github.com/WOLFraMix/aranea/internal/fetcher"
	"github.com/redis/go-redis/v9"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.New()

	prod, err := fetcher.NewProducer(cfg.KafkaAddr)
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer prod.Close()

	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "127.0.0.1:6379")})

	consumer, err := fetcher.NewConsumer(cfg.KafkaAddr)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	pool := fetcher.New(cfg,
		fetcher.NewProbe(cfg.ProbeTimeout),
		fetcher.NewDomainLimits(cfg.RatePerSec),
		prod,
		fetcher.NewRobots(cfg.ProbeTimeout),
		rdb,
	)

	consumerDone, err := consumer.ConsumeTasks(ctx, pool.Tasks())
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}

	log.Println("fetcher: running")
	pool.Run(ctx)  // живёт до Ctrl+C, потом сам зачищает пул
	<-consumerDone // дожидаемся консюмера

	log.Println("fetcher: exiting")
}
