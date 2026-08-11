package main

import (
	"errors"
	"time"

	"github.com/Ivanvnew75/libs/common"
)

type Config struct {
	Port string

	AnswersURL     string
	AnalyticsURL   string
	RecommenderURL string

	// LinkSecret — общий секрет с telegram-api для подписи ссылок входа.
	// Тот же ключ используют оба сервиса, поэтому он лежит в одном
	// Secret и монтируется обоим. В задании 15 переедет в Vault.
	LinkSecret string
	SessionTTL time.Duration

	// SecureCookie — ставить ли флаг Secure на сессионную куку.
	// Вынесено в конфиг ради локальной разработки по HTTP:
	// с Secure=true браузер просто не сохранит куку на http://localhost,
	// и вход будет «молча не работать». В кластере всегда true.
	SecureCookie bool

	HTTPTimeout     time.Duration
	HTTPRetries     int
	ShutdownTimeout time.Duration
	LogLevel        string
	LogFormat       string
}

func loadConfig() (Config, error) {
	c := Config{
		Port:           common.Env("SERVER_PORT", "8080"),
		AnswersURL:     common.Env("ANSWERS_URL", "http://answers.moodbot.svc.cluster.local"),
		AnalyticsURL:   common.Env("ANALYTICS_URL", "http://analytics.moodbot.svc.cluster.local"),
		RecommenderURL: common.Env("RECOMMENDER_URL", "http://recommender.moodbot.svc.cluster.local"),
		SecureCookie:   common.Env("SECURE_COOKIE", "true") == "true",
		LogLevel:       common.Env("LOG_LEVEL", "info"),
		LogFormat:      common.Env("LOG_FORMAT", "json"),
	}

	var err error
	if c.LinkSecret, err = common.MustEnv("LINK_SECRET"); err != nil {
		return c, err
	}
	// Минимальная длина ключа проверяется на старте.
	// HMAC-SHA256 с коротким ключом формально работает, и именно поэтому
	// ошибку легко не заметить: подписи считаются, всё «функционирует».
	// Проверка превращает слабый ключ в отказ старта.
	if len(c.LinkSecret) < 32 {
		return c, errors.New("LINK_SECRET должен быть не короче 32 символов")
	}

	if c.SessionTTL, err = common.EnvDuration("SESSION_TTL", 168*time.Hour); err != nil {
		return c, err
	}
	if c.HTTPTimeout, err = common.EnvDuration("HTTP_TIMEOUT", 5*time.Second); err != nil {
		return c, err
	}
	if c.HTTPRetries, err = common.EnvInt("HTTP_RETRIES", 1); err != nil {
		return c, err
	}
	if c.ShutdownTimeout, err = common.EnvDuration("SHUTDOWN_TIMEOUT", 15*time.Second); err != nil {
		return c, err
	}
	return c, nil
}
