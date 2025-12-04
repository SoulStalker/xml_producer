package main

import (
	"fmt"

	"github.com/SoulStalker/xml_producer/internal/config"
)

func main() {
	// init config
	configPath := "./config/config.yml"

	cfg := config.MustLoad(configPath)
	fmt.Println(cfg.Kafka.Brokers[0])
	fmt.Println(cfg.Storage.GetRetentionDuration())

}