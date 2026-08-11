package main

import (
	"time"

	"github.com/Ivanvnew75/libs/common"
)

type Config struct {
	Port string

	AnalyticsURL string

	// LLMBaseURL — база OpenAI-совместимого API, БЕЗ /chat/completions.
	// Провайдер меняется одной переменной: api.openai.com/v1,
	// адрес прокси или локальная Ollama на 11434/v1.
	LLMBaseURL string
	LLMModel   string
	// LLMAPIKey пустой = внешний вызов выключен, сервис работает
	// на заготовленных текстах. Это НЕ деградация ради простоты,
	// а рабочий режим для разработки и для стенда без платного ключа.
	LLMAPIKey string
	// LLMTimeout заметно больше обычного HTTP_TIMEOUT: генерация
	// текста занимает секунды, и 5 секунд обрубали бы почти все
	// ответы. Расплата — этот сервис по определению медленный,
	// поэтому в кабинете он вызывается параллельно с остальными.
	LLMTimeout time.Duration

	RedisAddr string
	RedisDB   int
	CacheTTL  time.Duration

	HTTPTimeout     time.Duration
	HTTPRetries     int
	ShutdownTimeout time.Duration
	LogLevel        string
	LogFormat       string
}

func loadConfig() (Config, error) {
	c := Config{
		Port:         common.Env("SERVER_PORT", "8080"),
		AnalyticsURL: common.Env("ANALYTICS_URL", "http://analytics.moodbot.svc.cluster.local"),
		LLMBaseURL:   common.Env("LLM_BASE_URL", "https://api.openai.com/v1"),
		LLMModel:     common.Env("LLM_MODEL", "gpt-4o-mini"),
		LLMAPIKey:    common.Env("LLM_API_KEY", ""),
		RedisAddr:    common.Env("REDIS_ADDR", "redis.moodbot.svc.cluster.local:6379"),
		LogLevel:     common.Env("LOG_LEVEL", "info"),
		LogFormat:    common.Env("LOG_FORMAT", "json"),
	}

	var err error
	// Отдельная база Redis, а не общая со scheduler.
	// В Redis нет прав доступа по ключам, но есть логические базы:
	// FLUSHDB в одной не тронет другую, и ключи не столкнутся.
	// Слабое, но бесплатное разделение.
	if c.RedisDB, err = common.EnvInt("REDIS_DB", 1); err != nil {
		return c, err
	}
	if c.CacheTTL, err = common.EnvDuration("CACHE_TTL", 24*time.Hour); err != nil {
		return c, err
	}
	if c.LLMTimeout, err = common.EnvDuration("LLM_TIMEOUT", 20*time.Second); err != nil {
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
