// Сервис analytics — второй потребитель топика answers.v1.
//
// ПОЧЕМУ ОТДЕЛЬНЫЙ СЕРВИС, А НЕ ЕЩЁ ОДИН ЗАПРОС ВНУТРИ answers.
// Потому что у них разные свойства отказа. answers — источник истины:
// его недоступность означает потерю истории пользователя. analytics —
// производное хранилище: он может отставать на час, лежать сутки и быть
// восстановлен переигрыванием топика. Смешать их в одном процессе значит
// приравнять надёжность источника истины к надёжности графиков.
//
// Разные consumer group — ключевая деталь: каждый потребитель ведёт
// СВОЙ offset. Падение analytics не тормозит answers, и наоборот.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Ivanvnew75/libs/common"
	"github.com/Ivanvnew75/libs/events"
	"github.com/Ivanvnew75/libs/kafkax"
	"github.com/labstack/echo/v4"
	"github.com/segmentio/kafka-go"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	logger := common.NewLogger("analytics", version, cfg.LogFormat, cfg.LogLevel)
	ctx, stop := common.SignalContext()
	defer stop()

	store, err := NewStore(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDB,
		cfg.ClickHouseUser, cfg.ClickHousePassword, cfg.ClickHouseTLS)
	if err != nil {
		logger.Error("clickhouse connect failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer store.Close()

	// Подкоманда migrate — тот же образ, тот же релиз (Фактор 12).
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := applySchema(ctx, store.Conn(), logger); err != nil {
			logger.Error("schema failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		return
	}

	logger.Info("starting",
		slog.String("commit", commit),
		slog.String("clickhouse", cfg.ClickHouseAddr),
		slog.String("group", cfg.KafkaGroup))

	metrics := common.NewMetrics("analytics")
	appMetrics := newAppMetrics(metrics.Registry())

	consumer := kafkax.NewConsumer(kafkax.ConsumerConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    events.TopicAnswers,
		Group:    cfg.KafkaGroup,
		DLQTopic: events.TopicAnswersDLQ,
		Log:      logger,
	})
	defer consumer.Close()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Run(ctx, handler(store, logger, appMetrics)); err != nil {
			logger.Error("consumer failed", slog.String("error", err.Error()))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				appMetrics.lag.Set(float64(consumer.Lag()))
			}
		}
	}()

	e := newAPI(store, logger, metrics)
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

	<-ctx.Done()
	logger.Info("shutdown signal received")
	shutdownCtx, cancel := common.ShutdownContext(cfg.ShutdownTimeout)
	defer cancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.String("error", err.Error()))
	}
	wg.Wait()
	logger.Info("stopped")
}

func handler(store *Store, logger *slog.Logger, m *appMetrics) kafkax.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		e, err := events.ParseAnswerReceived(msg.Value)
		if err != nil {
			m.failed.Inc()
			return kafkax.Permanent(err)
		}

		if err := store.Insert(ctx, e); err != nil {
			m.failed.Inc()
			// Недоступность ClickHouse — временная беда. Ретраим.
			// Важно: analytics при этом НЕ мешает сервису answers,
			// у них разные consumer group и разные offset.
			return err
		}

		m.inserted.Inc()
		logger.Debug("событие записано в ClickHouse",
			slog.String("event_id", e.EventID),
			slog.String("request_id", e.RequestID))
		return nil
	}
}

func newAPI(store *Store, logger *slog.Logger, metrics *common.Metrics) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(common.RequestID(), common.PropagateRequestID(),
		common.RequestLogger(logger), metrics.Middleware())
	metrics.Register(e)

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	e.GET("/ready", func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 3*time.Second)
		defer cancel()
		if err := store.Ping(ctx); err != nil {
			return c.JSON(http.StatusServiceUnavailable,
				map[string]string{"status": "clickhouse unavailable"})
		}
		return c.JSON(http.StatusOK, map[string]string{"status": "ready"})
	})

	e.GET("/analytics/daily", func(c echo.Context) error {
		userID, err := userIDParam(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		// Потолок окна. Без него ?days=100000 заставит ClickHouse
		// прочитать все партиции таблицы — дешёвый способ положить
		// сервис одним GET-запросом (вектор для задания 17).
		days := 30
		if v := c.QueryParam("days"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				days = min(n, 365)
			}
		}
		points, err := store.Daily(c.Request().Context(), userID, days)
		if err != nil {
			logger.Error("daily failed", slog.String("error", err.Error()))
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "internal error"})
		}
		return c.JSON(http.StatusOK, points)
	})

	e.GET("/analytics/summary", func(c echo.Context) error {
		userID, err := userIDParam(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		sm, err := store.SummaryFor(c.Request().Context(), userID)
		if err != nil {
			logger.Error("summary failed", slog.String("error", err.Error()))
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "internal error"})
		}
		return c.JSON(http.StatusOK, sm)
	})

	return e
}

func userIDParam(c echo.Context) (int64, error) {
	id, err := strconv.ParseInt(c.QueryParam("user_id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("user_id is required")
	}
	return id, nil
}
