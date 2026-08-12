package main

import (
	"strings"
	"time"

	"github.com/Ivanvnew75/libs/common"
)

type Config struct {
	Port string

	KafkaBrokers []string
	KafkaGroup   string
	// Учётные данные SASL. Пустой пользователь = подключение без
	// аутентификации (слушатель 9092) — режим на время миграции.
	KafkaUser     string
	KafkaPassword string

	ClickHouseAddr     string
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string
	// ClickHouseTLS — на стенде выключен (внутрикластерный трафик
	// не шифруется), но переменная есть: в проде между сервисом и БД
	// обязан быть TLS, и это не должно требовать правки кода.
	ClickHouseTLS bool

	ShutdownTimeout time.Duration
	LogLevel        string
	LogFormat       string
}

func loadConfig() (Config, error) {
	c := Config{
		Port:           common.Env("SERVER_PORT", "8080"),
		KafkaGroup:     common.Env("KAFKA_GROUP", "analytics-writer"),
		ClickHouseAddr: common.Env("CLICKHOUSE_ADDR", "clickhouse.moodbot.svc.cluster.local:9000"),
		ClickHouseDB:   common.Env("CLICKHOUSE_DB", "moodbot"),
		ClickHouseUser: common.Env("CLICKHOUSE_USER", "analytics"),
		ClickHouseTLS:  common.Env("CLICKHOUSE_TLS", "false") == "true",
		KafkaUser:      common.Env("KAFKA_USER", ""),
		KafkaPassword:  common.Env("KAFKA_PASSWORD", ""),
		LogLevel:       common.Env("LOG_LEVEL", "info"),
		LogFormat:      common.Env("LOG_FORMAT", "json"),
	}

	var err error
	if c.ClickHousePassword, err = common.MustEnv("CLICKHOUSE_PASSWORD"); err != nil {
		return c, err
	}
	brokers, err := common.MustEnv("KAFKA_BROKERS")
	if err != nil {
		return c, err
	}
	c.KafkaBrokers = strings.Split(brokers, ",")

	if c.ShutdownTimeout, err = common.EnvDuration("SHUTDOWN_TIMEOUT", 15*time.Second); err != nil {
		return c, err
	}
	return c, nil
}
