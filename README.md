# XML producer


## Описание

Универсальный сервис на Go для обработки XML файлов из NFS хранилища и отправки их в message broker (Kafka или RabbitMQ) с поддержкой retry, idempotency и Dead Letter Queue.

Возможности
- Поддержка двух брокеров: Kafka и RabbitMQ (переключение через конфигурацию)
- Retry механизм: Настраиваемое количество попыток с экспоненциальным backoff
- Idempotency: Гарантия отсутствия дубликатов через уникальные ключи на основе содержимого
- Dead Letter Queue (DLQ): Автоматическая отправка проблемных сообщений в отдельную очередь
- Файловый менеджмент: Автоматическое архивирование и очистка старых файлов
- Graceful shutdown: Корректная остановка с завершением текущих операций
- Гибкая конфигурация: TOML файл + переменные окружения

Архитектура
```text
┌──────────────┐      ┌──────────────────┐      ┌─────────────────┐
│  NFS Storage │─────▶│ File Processor   │─────▶│ Message Broker  │
│  (XML files) │      │                  │      │ (Kafka/RabbitMQ)│
└──────────────┘      └──────────────────┘      └─────────────────┘
                             │                           │
                             │                           │
                             ▼                           ▼
                      ┌─────────────┐            ┌─────────────┐
                      │   Backup    │            │     DLQ     │
                      │  Directory  │            │  (failures) │
                      └─────────────┘            └─────────────┘
```

Компоненты
- Config Layer: Загрузка конфигурации из TOML и environment variables
- Broker Interface: Общий интерфейс для всех message brokers
- Kafka Producer: Реализация для Apache Kafka с compression и partitioning
- RabbitMQ Producer: Реализация для RabbitMQ с publisher confirms
- File Processor: Сканирование, обработка и архивирование XML файлов

## Конфигурация

Конфигурация задаётся через файл `config.yaml` и может быть переопределена переменными окружения.

### Пример `config.yaml`

```yaml
broker:
  type: "rabbitmq"
rabbit-mq:
  url: "amqp://guest:guest@localhost:5672/"
  exchange_type: "topic"
  routing_key: "xml.file"
  dlq_routing_key: "xml.file.failed"
  dlq_exchange: "xml-files-dlq"
  max_retries: 3
  retry_backoff_ms: 1000
  durable: true
  persistent: true
kafka:
  brokers:
    - "localhost:9092"
    - "localhost:9093"
  topic: "xml-files"
  dlq_topic: "xml-files-dlq"
  max_retries: 3
  retry_backoff_ms: 1000
  batch_timeout_ms: 100
  compression: "gzip"

storage:
  nfs_path: "/mnt/nfs/xml_files"
  backup_path: "/mnt/nfs/backup"
  backup_retention_days: 30

app:
  poll_interval_sec: 10
  log_level: "info"
```

### 

Если переменная не задана, используются значения из `config.yaml` или дефолты, прописанные в структурах конфигурации.

## Запуск

### Локальный запуск

```bash
# С Kafka
export BROKER_TYPE=kafka
go run ./cmd/main.go

# С RabbitMQ
export BROKER_TYPE=rabbitmq
go run ./cmd/main.go
```

### Сборка бинарника

```bash
go build -o message-broker-producer ./cmd/main.go
./message-broker-producer
```

### Docker

```text
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o producer ./cmd/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/producer .
COPY config.toml .
CMD ["./producer"]
```

```bash
docker build -t message-broker-producer .
docker run -v /mnt/nfs:/mnt/nfs -v $(pwd)/config.toml:/root/config.toml message-broker-producer
```

### Как это работает

1. Обработка файлов
Сервис периодически (каждые N секунд) сканирует NFS директорию:
- Находит все .xml файлы
- Читает содержимое каждого файла
- Отправляет в message broker
- При успехе перемещает файл в backup
- Логирует все операции

2. Idempotency механизм
Для каждого файла генерируется уникальный ключ:

```text
idempotency_key = MD5(filename + file_content)
```
### В Kafka:

- Ключ используется как message key
- Kafka гарантирует порядок сообщений с одним ключом в партиции
- Consumer может использовать ключ для дедупликации

### В RabbitMQ:

- Ключ записывается в MessageId поле
- Добавляется в headers как idempotency-key
- Consumer может проверять дубликаты по этому полю

3. Retry стратегия

При ошибке отправки:

- Первая попытка: немедленно
- Вторая попытка: через retry_backoff_ms * 1
- Третья попытка: через retry_backoff_ms * 2
- И так далее до max_retries
- Если все попытки исчерпаны → отправка в DLQ.

4. Dead Letter Queue

Сообщения попадают в DLQ когда:

- Исчерпаны все retry попытки
- Broker недоступен
- Timeout при публикации

DLQ сообщение содержит:
- Оригинальные данные
- Оригинальные headers
- dlq-reason - причина ошибки
- dlq-timestamp - время помещения в DLQ

5. Cleanup старых файлов
Каждый цикл обработки:

- Проверяет файлы в backup директории
- Сравнивает mtime с текущим временем
- Удаляет файлы старше backup_retention_days
- Логирует количество удаленных файлов

Сравнение Kafka vs RabbitMQ

| Параметр           | Kafka                   | RabbitMQ             |
| ------------------ | ----------------------- | -------------------- |
| Тип                | Distributed log         | Message broker       |
| Производительность | Очень высокая           | Высокая              |
| Persistence        | По умолчанию            | Опционально          |
| Ordering           | По партиции             | По очереди           |
| Consumer groups    | Да                      | Да (через exchanges) |
| Message TTL        | Через retention         | Да                   |
| Идеально для       | Streaming, logs, events | Task queues, RPC     |

### Когда использовать Kafka

- Нужен replay сообщений
- Event streaming и log aggregation
- Множество consumers читают одни данные

Когда использовать RabbitMQ
- Сложный routing (topic, headers exchanges)
- Priority queues
- RPC паттерны
- Гарантии доставки важнее throughput

### Мониторинг
### Логи
Сервис логирует:

- Успешные отправки сообщений
- Ошибки и retry попытки
- Отправки в DLQ с причинами
- Перемещение файлов в backup
- Cleanup операции

Пример логов:

```text
2025/12/11 17:30:15 Message Broker Producer started (broker: kafka)
2025/12/11 17:30:20 Successfully sent message to Kafka for file: document1.xml
2025/12/11 17:30:20 Successfully processed and backed up file: document1.xml
2025/12/11 17:30:25 Kafka retry attempt 2/3 after 1s
2025/12/11 17:30:30 Failed to send message after retries: connection refused
2025/12/11 17:30:30 Deleted old backup file: old_document.xml (age: 720h)
```

### Метрики (опционально можно добавить)

Рекомендуется добавить:

- Prometheus metrics для количества processed/failed файлов
- Grafana dashboard для визуализации
- Health check endpoint

Расширение функциональности
Добавление нового брокера
1. Создай новую директорию internal/broker/newbroker/
2. Реализуй интерфейс MessageProducer:

```go
type MessageProducer interface {
    SendMessage(ctx context.Context, fileName string, data []byte) error
    Close() error
}
```

3. Добавь конфигурацию в config.go
4. Добавь создание producer в main.go

Добавление других типов файлов
Измени фильтр в file_processor.go:

```go
// Вместо только .xml
validExtensions := []string{".xml", ".json", ".csv"}
```

Добавление трансформации данных
Добавь middleware между чтением файла и отправкой:

```go
func (fp *FileProcessor) transformData(data []byte) ([]byte, error) {
    // Валидация XML
    // Обогащение данными
    // Конвертация формата
    return transformedData, nil
}
```


Лицензия
MIT

Контакты
Для вопросов и предложений создавайте issues в репозитории.