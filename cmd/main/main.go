package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SoulStalker/xml_producer/internal/config"
	"github.com/SoulStalker/xml_producer/internal/processor"
	"github.com/SoulStalker/xml_producer/internal/producer"
)

func createKafkaProducer(cfg *config.Config) producer.MessageProducer {
	compression := producer.GetCompression(cfg.Kafka.Compression)
	producerConfig := producer.KafkaConfig{
		Brokers:      cfg.Kafka.Brokers,
		Topic:        cfg.Kafka.Topic,
		DLQTopic:     cfg.Kafka.DLQTopic,
		MaxRetries:   cfg.Kafka.MaxRetries,
		RetryBackoff: time.Duration(cfg.Kafka.RetryBackoffMs) * time.Millisecond,
		BatchTimeout: time.Duration(cfg.Kafka.BatchTimeoutMs) * time.Millisecond,
		Compression:  compression,
	}

	return producer.NewKafkaProducer(producerConfig)
}

func createRabbitMQProducer(cfg *config.Config) (producer.MessageProducer, error) {
	producerCfg := producer.RabbitMQConfig{
		URL:           cfg.RabbitMQ.URL,
		Exchange:      cfg.RabbitMQ.Exchange,
		ExchangeType:  cfg.RabbitMQ.ExchangeType,
		RoutingKey:    cfg.RabbitMQ.RoutingKey,
		DLQExchange:   cfg.RabbitMQ.DLQExchange,
		DLQRoutingKey: cfg.RabbitMQ.DLQRoutingKey,
		MaxRetries:    cfg.RabbitMQ.MaxRetries,
		RetryBackoff:  time.Duration(cfg.RabbitMQ.RetryBackoffMs) * time.Millisecond,
		Durable:       cfg.RabbitMQ.Durable,
		Persistent:    cfg.RabbitMQ.Persistent,
	}

	return producer.NewRabbitMQProducer(producerCfg)
}

func main() {

	configPath := flag.String("config", "./config/config.yaml", "config file (example: ./config/config.yaml)")

	cfg := config.MustLoad(*configPath)

	var p producer.MessageProducer
	var err error
	switch cfg.Broker.Type {
	case "kafka":
		p = createKafkaProducer(cfg)
		log.Println("Using Kafka as message broker")
	case "rabbitmq":
		p, err = createRabbitMQProducer(cfg)
		if err != nil {
			log.Fatalf("Failed to create RabbitMQ producer: %v", err)
		}
		log.Println("Using RabbitMQ as message broker")
	default:
		log.Fatalf("Unknown broker type: %s", cfg.Broker.Type)
	}
	defer p.Close()

	fileProc, err := processor.NewFileProcessor(
		cfg.Storage.NFSPath,
		cfg.Storage.BackupPath,
		cfg.Storage.GetRetentionDuration(),
		p,
	)
	if err != nil {
		log.Fatalf("Failed to create file processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(time.Duration(cfg.App.PollIntervalSec) * time.Second)
	defer ticker.Stop()

	log.Println("Kafka XML Producer started")

	if err := fileProc.CleanupOldBackups(); err != nil {
		log.Printf("Initial cleanup failed: %v", err)
	}

	for {
		select {
		case <-ticker.C:
			if err := fileProc.Process(ctx); err != nil {
				log.Printf("Error processing files: %v", err)
			}

			if err := fileProc.CleanupOldBackups(); err != nil {
				log.Printf("Cleanup error: %v", err)
			}
		case <-sigChan:
			log.Println("Shutting down gracefully...")
			cancel()
			return
		}
	}
}
