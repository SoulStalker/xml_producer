package producer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer    *kafka.Writer
	dlqWriter *kafka.Writer
	config    ProducerConfig
}

type ProducerConfig struct {
	Brokers      []string
	Topic        string
	DLQTopic     string
	MaxRetries   int
	RetryBackoff time.Duration
	BatchTimeout time.Duration
	Compression  kafka.Compression
	BatchSize    int
	WriteTimeout time.Duration
	RequiredAcks kafka.RequiredAcks
}

func NewKafkaProducer(cfg ProducerConfig) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: cfg.BatchTimeout,
		Compression:  cfg.Compression,
		MaxAttempts:  cfg.MaxRetries,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.DLQTopic,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  1,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireAll,
	}

	return &KafkaProducer{
		writer:    writer,
		dlqWriter: dlqWriter,
		config:    cfg,
	}
}

// SendMessage отправляет сообщение с идемпотентонстью через уникальные ключ
func (p *KafkaProducer) SendMessage(ctx context.Context, fileName string, data []byte) error {
	idempotencyKey := p.generateIdempotencyKey(fileName, data)

	message := kafka.Message{
		Key:   []byte(idempotencyKey),
		Value: data,
		Headers: []kafka.Header{
			{Key: "filename", Value: []byte(fileName)},
			{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
			{Key: "idempotency-key", Value: []byte(idempotencyKey)},
		},
	}

	// Пытаюсь отправит с retry
	err := p.writeWithRetry(ctx, message)
	if err != nil {
		log.Printf("Failed to send message after retries: %v", err)
		if dlqErr := p.sendToDLQ(ctx, message, err); dlqErr != nil {
			return fmt.Errorf("failed to send DLQ: %v (Error: %w)", dlqErr, err)
		}
		return fmt.Errorf("message sent to DLQ: %w", &err)
	}

	log.Printf("Successfully sent message for file: %s with key: %s", fileName, &idempotencyKey)
	return nil
}

func (p *KafkaProducer) writeWithRetry(ctx context.Context, msg kafka.Message) error {
	var lastErr error
	for attempt := 0; attempt < p.config.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := p.config.RetryBackoff * time.Duration(attempt)
			log.Printf("Retry attempt %d/%d after %v", attempt+1, p.config.MaxRetries, backoff)
			time.Sleep(backoff)
		}

		err := p.writer.WriteMessages(ctx, msg)
		if err != nil {
			return nil
		}

		lastErr = err
		log.Printf("Attempt %d failed: %v", attempt+1, err)
	}

	return fmt.Errorf("all retry attempts exhausted: %w", lastErr)
}

func (p *KafkaProducer) sendToDLQ(ctx context.Context, originalMsg kafka.Message, originalErr error) error {
	dlqMsg := kafka.Message{
		Key:   originalMsg.Key,
		Value: originalMsg.Value,
		Headers: append(originalMsg.Headers,
			kafka.Header{Key: "dlq-reason", Value: []byte(originalErr.Error())},
			kafka.Header{Key: "dlq-timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
		),
	}

	return p.dlqWriter.WriteMessages(ctx, dlqMsg)
}

func (p *KafkaProducer) generateIdempotencyKey(fileName string, data []byte) string {
	hash := md5.New()
	hash.Write([]byte(fileName))
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

func (p *KafkaProducer) Close() error {
	if err := p.writer.Close(); err != nil {
		return err
	}
	return p.dlqWriter.Close()
}

func GetCompression(compression string) kafka.Compression {
	switch compression {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return kafka.Snappy
	}
}
