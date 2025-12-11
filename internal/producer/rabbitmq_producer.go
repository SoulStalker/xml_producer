package producer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  RabbitConfig
}

type RabbitConfig struct {
	URL           string
	Exchange      string
	ExchangeType  string
	RoutingKey    string
	DLQExchange   string
	DLQRoutingKey string
	MaxRetries    int
	RetryBackoff  time.Duration
	Durable       bool
	Persistent    bool
}

func NewRabbitMQProducer(cfg RabbitConfig) (*RabbitMQProducer, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %v", err)
	}

	err = channel.ExchangeDeclare(
		cfg.Exchange,
		cfg.ExchangeType,
		cfg.Durable,
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %v", err)
	}

	// Объявляем DLQ exchange
	err = channel.ExchangeDeclare(
		cfg.DLQExchange,
		cfg.ExchangeType,
		cfg.Durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare DLQ exchange: %v", err)
	}

	err = channel.Confirm(false)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to put channel in confirm mode: %v", err)
	}

	return &RabbitMQProducer{
		conn:    conn,
		channel: channel,
		config:  cfg,
	}, nil
}

func (p *RabbitMQProducer) SendMessage(ctx context.Context, fileName string, data []byte) error {
	idempotencyKey := generateIdempotencyKey(fileName, data)

	deliveryMode := amqp.Transient
	if p.config.Persistent {
		deliveryMode = amqp.Persistent
	}

	publishing := amqp.Publishing{
		DeliveryMode: deliveryMode,
		ContentType:  "application/xml",
		Body:         data,
		MessageId:    idempotencyKey,
		Timestamp:    time.Now(),
		Headers: amqp.Table{
			"filename":        fileName,
			"idempotency-key": idempotencyKey,
			"content-type":    "application/xml",
		},
	}

	err := p.publishWithRetry(ctx, p.config.Exchange, p.config.RoutingKey, publishing)
	if err != nil {
		log.Printf("Failed to publish message after retries: %v", err)
		if dlqErr := p.sendToDLQ(ctx, publishing, err); dlqErr != nil {
			return fmt.Errorf("failed to send to DLQ: %v (original error: %v)", dlqErr, err)
		}
		return fmt.Errorf("message sent to DLQ: %v", err)
	}

	log.Printf("Successfully sent message to RabbitMQ for file: %s", fileName)
	return nil
}

func (p *RabbitMQProducer) publishWithRetry(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	var lastErr error

	for attempt := 0; attempt < p.config.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := p.config.RetryBackoff * time.Duration(attempt)
			log.Printf("RabbitMQ retry attempt %d/%d after %v", attempt+1, p.config.MaxRetries, backoff)
			time.Sleep(backoff)
		}

		confirms := p.channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		err := p.channel.PublishWithContext(
			ctx,
			exchange,
			routingKey,
			false, // mandatory
			false, // immediate
			msg,
		)

		if err != nil {
			lastErr = err
			log.Printf("RabbitMQ attempt %d failed: %v", attempt+1, err)
			continue
		}

		// Ждем подтверждения
		select {
		case confirm := <-confirms:
			if confirm.Ack {
				return nil
			}
			lastErr = fmt.Errorf("publish not confirmed by broker")
			log.Printf("RabbitMQ attempt %d: message not confirmed", attempt+1)
		case <-time.After(5 * time.Second):
			lastErr = fmt.Errorf("confirmation timeout")
			log.Printf("RabbitMQ attempt %d: confirmation timeout", attempt+1)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("all retry attempts exhausted: %v", lastErr)
}

func (p *RabbitMQProducer) sendToDLQ(ctx context.Context, originalMsg amqp.Publishing, originalErr error) error {
	dlqMsg := originalMsg
	if dlqMsg.Headers == nil {
		dlqMsg.Headers = amqp.Table{}
	}
	dlqMsg.Headers["dlq-reason"] = originalErr.Error()
	dlqMsg.Headers["dlq-timestamp"] = time.Now().Format(time.RFC3339)

	confirms := p.channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	err := p.channel.PublishWithContext(
		ctx,
		p.config.DLQExchange,
		p.config.DLQRoutingKey,
		false,
		false,
		dlqMsg,
	)

	if err != nil {
		return err
	}

	// Ждем подтверждения
	select {
	case confirm := <-confirms:
		if confirm.Ack {
			return nil
		}
		return fmt.Errorf("DLQ publish not confirmed")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("DLQ confirmation timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *RabbitMQProducer) Close() error {
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			log.Printf("Error closing channel: %v", err)
		}
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

func generateIdempotencyKey(fileName string, data []byte) string {
	hash := md5.New()
	hash.Write([]byte(fileName))
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}
