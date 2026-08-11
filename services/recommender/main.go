// Сервис recommender — персональные рекомендации через внешний LLM API.
//
// ЧТО ЗДЕСЬ ИНТЕРЕСНО С ТОЧКИ ЗРЕНИЯ DEVOPS, А НЕ ML.
// Это единственный сервис, который ходит ЗА ПРЕДЕЛЫ кластера к чужому
// платному API. Все его особенности вытекают ровно из этого:
//
//	таймаут + размыкатель  — чужой сервис лежит и тормозит;
//	кэш в Redis            — чужой сервис берёт деньги за каждый запрос;
//	минимизация данных     — наружу уходят персональные данные;
//	заготовленный ответ    — отказ обязан деградировать, а не ломать;
//	отдельный egress       — в задании 14 наружу будет разрешено
//	                         ходить ТОЛЬКО этому сервису.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Ivanvnew75/libs/common"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var (
	version = "dev"
	commit  = "unknown"
)

const systemPrompt = `Ты — доброжелательный помощник в приложении,
которое раз в день спрашивает у человека о его настроении.
Тебе дают обобщённую статистику за последние недели.
Дай ОДИН короткий практический совет (2-3 предложения) на русском языке.
Не ставь диагнозов, не давай медицинских рекомендаций, не драматизируй.
Если данных мало — так и скажи и предложи продолжить отвечать боту.`

type app struct {
	llm       *LLM
	analytics *analyticsClient
	cache     *redis.Client
	cacheTTL  time.Duration
	log       *slog.Logger
	m         *appMetrics
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	logger := common.NewLogger("recommender", version, cfg.LogFormat, cfg.LogLevel)
	logger.Info("starting",
		slog.String("commit", commit),
		slog.String("llm_base_url", cfg.LLMBaseURL),
		slog.String("model", cfg.LLMModel),
		slog.Bool("llm_enabled", cfg.LLMAPIKey != ""))

	ctx, stop := common.SignalContext()
	defer stop()

	metrics := common.NewMetrics("recommender")
	a := &app{
		llm:       NewLLM(cfg.LLMBaseURL, cfg.LLMAPIKey, cfg.LLMModel, cfg.LLMTimeout, logger),
		analytics: newAnalyticsClient(cfg.AnalyticsURL, cfg.HTTPTimeout, cfg.HTTPRetries),
		cache:     redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB}),
		cacheTTL:  cfg.CacheTTL,
		log:       logger,
		m:         newAppMetrics(metrics.Registry()),
	}
	defer a.cache.Close()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(common.RequestID(), common.PropagateRequestID(),
		common.RequestLogger(logger), metrics.Middleware())
	metrics.Register(e)

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	// readiness НЕ проверяет внешний LLM.
	//
	// Иначе недоступность чужого API вывела бы наши поды из балансировки,
	// и вместо заготовленного совета пользователь получил бы 503.
	// Готовность — про способность обслужить запрос, а сервис способен:
	// у него есть запасной ответ.
	e.GET("/ready", func(c echo.Context) error {
		cctx, cancel := context.WithTimeout(c.Request().Context(), 2*time.Second)
		defer cancel()
		if err := a.cache.Ping(cctx).Err(); err != nil {
			// Redis — кэш, но его недоступность означает, что каждый
			// запрос пойдёт в платный API. Это достаточная причина
			// вывести под из ротации и разобраться.
			return c.JSON(http.StatusServiceUnavailable,
				map[string]string{"status": "cache unavailable"})
		}
		return c.JSON(http.StatusOK, map[string]string{"status": "ready"})
	})

	e.GET("/recommend", a.handleRecommend)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		addr := ":" + cfg.Port
		logger.Info("http server listening", slog.String("addr", addr))
		if err := e.Start(addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", slog.String("error", err.Error()))
			stop()
		}
	}()

	// Состояние размыкателя как метрика: без неё «почему рекомендаций
	// нет» выясняется чтением логов, а не взглядом на график.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if a.llm.breaker.state() == "open" {
					a.m.breakerOpen.Set(1)
				} else {
					a.m.breakerOpen.Set(0)
				}
			}
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := common.ShutdownContext(cfg.ShutdownTimeout)
	defer cancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.String("error", err.Error()))
	}
	wg.Wait()
	logger.Info("stopped")
}

type recommendation struct {
	Text string `json:"text"`
	// Source показывает, откуда взялся текст: llm, cache или fallback.
	// Это не отладочная информация — по ней видно, работает ли внешний
	// API вообще, причём видно и пользователю кабинета, и в метриках.
	Source string `json:"source"`
}

func (a *app) handleRecommend(c echo.Context) error {
	userID, err := strconv.ParseInt(c.QueryParam("user_id"), 10, 64)
	if err != nil || userID <= 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "user_id is required"})
	}
	ctx := c.Request().Context()
	cacheKey := fmt.Sprintf("rec:%d", userID)

	// 1. Кэш.
	//
	// Рекомендация основана на статистике за недели — она не меняется
	// от того, что человек обновил страницу. Кэш на сутки убирает
	// 99% обращений к платному API. Это не микрооптимизация: без кэша
	// каждый F5 в браузере стоит денег.
	if cached, err := a.cache.Get(ctx, cacheKey).Result(); err == nil && cached != "" {
		a.m.cacheHits.Inc()
		return c.JSON(http.StatusOK, recommendation{Text: cached, Source: "cache"})
	}
	a.m.cacheMisses.Inc()

	sm, err := a.analytics.Summary(ctx, userID)
	if err != nil {
		a.log.Warn("analytics недоступен", slog.String("error", err.Error()))
		return c.JSON(http.StatusOK, recommendation{Text: fallbackText(nil), Source: "fallback"})
	}

	if a.llm.apiKey == "" {
		// Ключа нет — сервис работает, но на заготовленных текстах.
		// Это ВАЖНОЕ свойство: отсутствие ключа не должно ронять
		// ни сервис, ни кабинет. Разработчик поднимает стенд без
		// платного ключа и видит рабочую систему.
		return c.JSON(http.StatusOK, recommendation{Text: fallbackText(&sm), Source: "fallback"})
	}

	// 2. Внешний вызов.
	//
	// МИНИМИЗАЦИЯ ДАННЫХ: наружу уходят ТОЛЬКО агрегаты — количество
	// ответов и средние оценки. Ни текстов ответов, ни telegram_id,
	// ни имени. Соблазн отправить последние ответы «для контекста»
	// велик и качество совета поднял бы, но это передача переписки
	// человека третьей стороне. Такое решение принимает не инженер
	// в одиночку, и в threat-модели (задание 10) оно отмечено отдельно.
	prompt := fmt.Sprintf(
		"Ответов всего: %d. Средняя оценка настроения за всё время: %.2f "+
			"(шкала от -2 до 2). За последние 7 дней: %.2f, за предыдущие 7 дней: %.2f. "+
			"Тенденция: %s.",
		sm.Total, sm.AvgScore, sm.Last7, sm.Prev7, sm.Trend)

	start := time.Now()
	text, err := a.llm.Complete(ctx, systemPrompt, prompt)
	a.m.llmDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		a.m.llmErrors.Inc()
		if errors.Is(err, ErrCircuitOpen) {
			// Ожидаемое состояние при лежащем провайдере — не ERROR.
			// Иначе алерт «много ошибок» будет срабатывать на штатную
			// защиту, и на него перестанут смотреть.
			a.log.Info("цепь разомкнута, отдаю заготовленный текст")
		} else {
			a.log.Error("llm failed", slog.String("error", err.Error()))
		}
		return c.JSON(http.StatusOK, recommendation{Text: fallbackText(&sm), Source: "fallback"})
	}

	a.m.llmCalls.Inc()
	// Кэш пишем «как получится»: ошибка записи в кэш не должна
	// портить успешный ответ пользователю.
	if err := a.cache.Set(ctx, cacheKey, text, a.cacheTTL).Err(); err != nil {
		a.log.Warn("не удалось записать кэш", slog.String("error", err.Error()))
	}
	return c.JSON(http.StatusOK, recommendation{Text: text, Source: "llm"})
}

// fallbackText — что показать, когда внешнего совета нет.
//
// Не «сервис недоступен». Пользователю всё равно, какой у нас
// провайдер: текст должен быть осмысленным сам по себе, просто
// не персонализированным. Разница между «ошибка» и «общий совет» —
// это разница между сломанным продуктом и продуктом попроще.
func fallbackText(sm *Summary) string {
	if sm == nil || sm.Total == 0 {
		return "Пока мало данных. Отвечайте боту хотя бы несколько дней подряд — " +
			"тогда появится динамика, по которой можно что-то посоветовать."
	}
	switch {
	case sm.Last7 > 0.5:
		return "Последняя неделя выглядит неплохо. Обратите внимание, что именно " +
			"её отличало от предыдущих — такие вещи полезно замечать и повторять."
	case sm.Last7 < -0.5:
		return "Неделя выдалась тяжёлой. Самое полезное сейчас — базовые вещи: " +
			"сон, прогулка, разговор с близким человеком. Если так уже долго — стоит обратиться к специалисту."
	default:
		return "Настроение держится ровно. Продолжайте отмечать его каждый день: " +
			"на длинной дистанции видны закономерности, незаметные изо дня в день."
	}
}

// ── метрики ─────────────────────────────────────────────────────────

type appMetrics struct {
	llmCalls    prometheus.Counter
	llmErrors   prometheus.Counter
	llmDuration prometheus.Histogram
	cacheHits   prometheus.Counter
	cacheMisses prometheus.Counter
	breakerOpen prometheus.Gauge
}

func newAppMetrics(reg *prometheus.Registry) *appMetrics {
	m := &appMetrics{
		llmCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "recommender_llm_calls_total",
			Help: "Успешных обращений к внешнему LLM API (каждое стоит денег)",
		}),
		llmErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "recommender_llm_errors_total",
			Help: "Неудачных обращений к внешнему LLM API",
		}),
		llmDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "recommender_llm_duration_seconds",
			Help: "Длительность обращения к внешнему LLM API",
			// Границы под ВНЕШНИЙ вызов: дефолтные бакеты Prometheus
			// заканчиваются на 10 секундах и рассчитаны на быстрые
			// внутренние запросы. LLM отвечает секундами, и в дефолтных
			// бакетах вся картина слиплась бы в последний.
			Buckets: []float64{0.5, 1, 2, 5, 10, 20, 30},
		}),
		cacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "recommender_cache_hits_total",
			Help: "Ответов, отданных из кэша",
		}),
		cacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "recommender_cache_misses_total",
			Help: "Промахов кэша",
		}),
		breakerOpen: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "recommender_breaker_open",
			Help: "1 — размыкатель цепи разомкнут, внешний API считается недоступным",
		}),
	}
	reg.MustRegister(m.llmCalls, m.llmErrors, m.llmDuration,
		m.cacheHits, m.cacheMisses, m.breakerOpen)
	return m
}
