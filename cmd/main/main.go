package main

import (
	"fmt"

	"github.com/SoulStalker/xml_producer/internal/config"
)

func main() {
	// init config
	cfg := config.MustLoad()
	fmt.Println(cfg.Kafka.Brokers[0])
	fmt.Println(cfg.Storage.GetRetentionDuration())

}