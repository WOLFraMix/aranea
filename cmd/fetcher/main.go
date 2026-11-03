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
	// signal.NotifyContext: Ctrl+C или SIGTERM из docker stop превращаются
	// в отмену ctx — дальше вся отмена идёт через контекст, без глобальных флагов
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.New()

	prod, err := fetcher.NewProducer(cfg.KafkaAddr)
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer prod.Close()

	pool := fetcher.New(cfg,
		fetcher.NewProbe(cfg.ProbeTimeout),
		fetcher.NewDomainLimits(cfg.RatePerSec),
		prod,
	)

	// TODO: создать консюмер и запустить в отдельной
	// горутине; затем pool.Run(ctx); дождаться завершения и залогировать выход.

	log.Println("fetcher: exiting")
}
