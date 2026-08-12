// Сервис answers — владелец истории ответов пользователей.
//
// ДВА ТИПА РАБОТЫ В ОДНОМ ПРОЦЕССЕ:
//   - потребитель Kafka (пишет),
//   - HTTP API (читает).
//
// Почему не два отдельных деплоймента, как у telegram-api (web + poller).
// У telegram-api разделение вынужденное: опросчик Telegram обязан быть
// в одном экземпляре, а web масштабируется свободно — разные свойства
// масштабирования требуют разных Deployment. Здесь обе роли масштабируются
// одинаково: consumer group сама распределит партиции между репликами,
// а HTTP-запросы разложит Service. Разделять нечего, и лишний Deployment
// был бы ровно тем «энтерпрайз-обвесом», от которого предостерегает курс.
//
// Потолок масштабирования при этом задан числом партиций (3):
// четвёртая реплика получит HTTP-трафик, но не получит партицию.
package main

import (
	"context"
	"embed"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Ivanvnew75/libs/common"
	"github.com/labstack/echo/v4"
	"github.com/segmentio/kafka-go"

	"github.com/Ivanvnew75/libs/events"
	"github.com/Ivanvnew75/libs/kafkax"
	"github.com/Ivanvnew75/moodbot-next/internal/pgmigrate"
)

var (
	version = "dev"
	commit  = "unknown"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Своё, зафиксированное число — см. комментарий в pgmigrate.Run.
const migrationLockID = 4812002

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	logger := common.NewLogger("answers", version, cfg.LogFormat, cfg.LogLevel)

	// Подкоманда migrate — Фактор 12 (Admin processes).
	// Тот же бинарник, тот же образ, то же окружение, что у сервиса.
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := pgmigrate.Run(context.Background(), cfg.DatabaseURL, "answers",
			migrationFS, "migrations", migrationLockID, logger); err != nil {
			logger.Error("migration failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		return
	}

	logger.Info("starting",
		slog.String("commit", commit),
		slog.String("kafka", cfg.KafkaBrokersRaw),
		slog.String("group", cfg.KafkaGroup))

	ctx, stop := common.SignalContext()
	defer stop()

	store, err := NewStore(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		logger.Error("db connect failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer store.Close()

	metrics := common.NewMetrics("answers")
	appMetrics := newAppMetrics(metrics.Registry())

	consumer := kafkax.NewConsumer(kafkax.ConsumerConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    events.TopicAnswers,
		Group:    cfg.KafkaGroup,
		DLQTopic: events.TopicAnswersDLQ,
		Log:      logger,
		// Аутентификация в Kafka. Пусто — значит слушатель без auth.
		SASLUser:     cfg.KafkaUser,
		SASLPassword: cfg.KafkaPassword,
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

	// Лаг потребителя как метрика Prometheus.
	//
	// Это ГЛАВНЫЙ показатель здоровья сервиса, и он не выводится
	// из состояния пода. Под может быть Running и Ready, а лаг —
	// расти часами: например, потребитель успешно читает, но каждое
	// сообщение уходит в ретраи из-за недоступной базы.
	// «Под Running» об этом не скажет ничего.
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

// handler — обработка одного сообщения из Kafka.
func handler(store *Store, logger *slog.Logger, m *appMetrics) kafkax.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		e, err := events.ParseAnswerReceived(msg.Value)
		if err != nil {
			m.failed.Inc()
			// Битый JSON и незнакомая версия схемы — НЕИСПРАВИМЫ.
			// Повторная попытка разобрать те же байты даст тот же
			// результат, сколько ни ретрай. Помечаем Permanent —
			// сообщение уедет в DLQ, а партиция продолжит работу.
			return kafkax.Permanent(err)
		}

		// request_id из события — в контекст и в лог, чтобы цепочка
		// «сообщение в Telegram → событие → строка в БД» искалась
		// одним фильтром через три сервиса.
		log := logger.With(
			slog.String("request_id", e.RequestID),
			slog.String("event_id", e.EventID),
			slog.Int64("user_id", e.UserID),
		)

		inserted, err := store.Save(ctx, e)
		if err != nil {
			m.failed.Inc()
			// Ошибку БД НЕ помечаем как постоянную: недоступность базы
			// временна, и сообщения этой минуты должны дождаться её,
			// а не уехать в DLQ.
			return err
		}

		if inserted {
			m.saved.Inc()
			log.Info("ответ сохранён")
		} else {
			m.duplicates.Inc()
			log.Info("дубль события, пропущен")
		}
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

	// liveness: процесс жив. Намеренно НЕ проверяет базу.
	// Если проверять зависимости в liveness, недоступность Postgres
	// приведёт к перезапуску всех подов answers — при том, что
	// перезапуск ничем не поможет, а лавину перезапусков устроит.
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	// readiness: готов обслуживать запросы. Здесь база проверяется —
	// без неё сервис не может ответить ни на один осмысленный запрос,
	// и его надо вывести из балансировки.
	e.GET("/ready", func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 2*time.Second)
		defer cancel()
		if err := store.Ping(ctx); err != nil {
			return c.JSON(http.StatusServiceUnavailable,
				map[string]string{"status": "db unavailable"})
		}
		return c.JSON(http.StatusOK, map[string]string{"status": "ready"})
	})

	e.GET("/answers", func(c echo.Context) error {
		userID, err := strconv.ParseInt(c.QueryParam("user_id"), 10, 64)
		if err != nil || userID <= 0 {
			return c.JSON(http.StatusBadRequest,
				map[string]string{"error": "user_id is required"})
		}

		// Лимит с потолком. Без верхней границы запрос ?limit=10000000
		// вытащит всю таблицу в память сервиса — это и деньги на трафик,
		// и готовый вектор DoS для задания 17.
		limit := 20
		if v := c.QueryParam("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				limit = min(n, 100)
			}
		}

		items, err := store.List(c.Request().Context(), userID, limit)
		if err != nil {
			logger.Error("list failed", slog.String("error", err.Error()))
			// Наружу — общая формулировка. Текст ошибки БД в теле ответа
			// раскрывает имена схем, таблиц и версию Postgres, то есть
			// готовую разведку для атакующего.
			return c.JSON(http.StatusInternalServerError,
				map[string]string{"error": "internal error"})
		}
		return c.JSON(http.StatusOK, items)
	})

	e.GET("/answers/count", func(c echo.Context) error {
		userID, err := strconv.ParseInt(c.QueryParam("user_id"), 10, 64)
		if err != nil || userID <= 0 {
			return c.JSON(http.StatusBadRequest,
				map[string]string{"error": "user_id is required"})
		}
		n, err := store.Count(c.Request().Context(), userID)
		if err != nil {
			return c.JSON(http.StatusInternalServerError,
				map[string]string{"error": "internal error"})
		}
		return c.JSON(http.StatusOK, map[string]int64{"count": n})
	})

	return e
}
