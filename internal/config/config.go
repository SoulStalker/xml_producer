package config

import (
	"log"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Kafka   KafkaConfig   `yaml:"kafka"`
	Storage StorageConfig `yaml:"storage"`
	App     AppConfig     `yaml:"app"`

	// Redis настройки
	RedisHost     string `yaml:"redis_host" env:"REDIS_HOST" env-default:"localhost"`
	RedisPort     string `yaml:"redis_port" env:"REDIS_PORT" env-default:"6379"`
	RedisPassword string `yaml:"redis_password" env:"REDIS_PASSWORD" env-default:""`
	RedisDB       int    `yaml:"redis_db" env:"REDIS_DB" env-default:"0"`
}

type KafkaConfig struct {
	Brokers        []string `yaml:"brokers" env-required:"true"`
	Topic          string   `yaml:"topic" env-required:"true"`
	DLQTopic       string   `yaml:"dlq_topic"`
	MaxRetries     int      `yaml:"max_retries" env-default:"3"`
	RetryBackoffMs int      `yaml:"retry_backoff_ms" env-default:"1000"`
	BatchTimeoutMs int      `yaml:"batch_timeout_ms" env-default:"100"`
	Compression    string   `yaml:"compression" env-default:"gzip"`
}

type StorageConfig struct {
	NFSPath             string `yaml:"nfs_path"`
	BackupPath          string `yaml:"backup_path"`
	BackupRetentionDays int    `yaml:"backup_retention_days" env-default:"30"`
}

type AppConfig struct {
	PollIntervalSec int    `yaml:"poll_interval_sec" env-default:"10"`
	LogLevel        string `yaml:"log_level" env-default:"info"`
}

func MustLoad() *Config {
	configPath := "./config/test_cfg.yml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("config file does not exits: %s", configPath)
	}

	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("cannot read config: %s", &err)
	}

	return &cfg
}

func (c *StorageConfig) GetRetentionDuration() time.Duration {
	return time.Duration(c.BackupRetentionDays) * 24 * time.Hour
}
