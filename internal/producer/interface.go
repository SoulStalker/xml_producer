package producer

import "context"

// MessageProducer - общий интерфейс для всех message brokers
type MessageProducer interface {
	SendMessage(ctx context.Context, fileName string, data []byte) error
	Close() error
}

// MessageHeaders содержит метаданные сообщения
type MessageHeaders struct {
	FileName       string
	Timestamp      string
	IdempotencyKey string
	ContentType    string
}

