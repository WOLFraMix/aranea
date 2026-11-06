// indexer: дедуп ссылок, хранение страниц, маршрутизация задач.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/WOLFraMix/aranea/internal/config"
	"github.com/WOLFraMix/aranea/internal/fetcher"
	"github.com/WOLFraMix/aranea/internal/indexer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.New()

	store, err := indexer.NewStore(cfg.RedisAddr, cfg.SQLitePath)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer store.Close()

	prod, err := fetcher.NewProducer(cfg.KafkaAddr)
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer prod.Close()

	router, err := indexer.New(cfg, store, prod)
	if err != nil {
		log.Fatalf("router: %v", err)
	}
	defer router.Close()

	log.Println("indexer: running")
	router.Run(ctx) // сам выйдет по отмене ctx — как pool.Run у fetcher'а
	log.Println("indexer: exiting")
}
