package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/Ivanvnew75/libs/common"
)

// Config — весь контракт сервиса с окружением в одном типе (Фактор 3).
type Config struct {
	Port string

	DatabaseURL string
	DBMaxConns  int32

	KafkaBrokersRaw string
	KafkaBrokers    []string
	// KafkaGroup вынесен в переменную окружения, а не захардкожен.
	//
	// Это не абстрактная гибкость: смена имени группы — штатный способ
	// ПЕРЕИГРАТЬ топик с начала (новая группа не имеет сохранённых
	// offset-ов и стартует с earliest). Понадобится при восстановлении
	// после инцидента.
	KafkaGroup string

	ShutdownTimeout time.Duration
	LogLevel        string
	LogFormat       string
}

func loadConfig() (Config, error) {
	c := Config{
		Port:       common.Env("SERVER_PORT", "8080"),
		KafkaGroup: common.Env("KAFKA_GROUP", "answers-writer"),
		LogLevel:   common.Env("LOG_LEVEL", "info"),
		LogFormat:  common.Env("LOG_FORMAT", "json"),
	}

	var err error
	// Без дефолтов: сервис без базы и без Kafka бессмыслен, и «умный
	// дефолт» вроде localhost привёл бы к тому, что в кластере под
	// стартует и молча ничего не делает. Fail fast понятнее.
	if c.DatabaseURL, err = common.MustEnv("DATABASE_URL"); err != nil {
		return c, err
	}
	if c.KafkaBrokersRaw, err = common.MustEnv("KAFKA_BROKERS"); err != nil {
		return c, err
	}
	c.KafkaBrokers = strings.Split(c.KafkaBrokersRaw, ",")

	maxConns, err := common.EnvInt("DB_MAX_CONNS", 5)
	if err != nil {
		return c, err
	}
	// Проверка границ перед сужением типа.
	//
	// Нашёл gosec (G115: integer overflow conversion int -> int32).
	// Находка настоящая: int32(maxConns) при DB_MAX_CONNS=3000000000
	// даёт ОТРИЦАТЕЛЬНОЕ число, и пул соединений создаётся с абсурдным
	// размером. Само по себе это не уязвимость — переменную задаём мы, —
	// но это ровно тот класс ошибок, из-за которого опечатка в ConfigMap
	// превращается в необъяснимое поведение в проде.
	//
	// Верхняя граница 100 не с потолка: у PostgreSQL по умолчанию
	// max_connections=100, и пул больше этого числа бессмыслен —
	// он лишь переносит отказ с приложения на базу.
	if maxConns < 1 || maxConns > 100 {
		return c, fmt.Errorf("DB_MAX_CONNS: ожидается 1..100, получено %d", maxConns)
	}
	c.DBMaxConns = int32(maxConns)

	if c.ShutdownTimeout, err = common.EnvDuration("SHUTDOWN_TIMEOUT", 15*time.Second); err != nil {
		return c, err
	}
	return c, nil
}
