// fetcher: качает страницы из очереди задач и публикует результаты.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/WOLFraMix/aranea/internal/config"
	"github.com/WOLFraMix/aranea/internal/fetcher"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.New()

	prod, err := fetcher.NewProducer(cfg.KafkaAddr)
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer prod.Close()

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
