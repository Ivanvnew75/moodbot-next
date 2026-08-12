// Сервис web-admin — единственная точка входа из интернета.
//
// Ровно поэтому он устроен максимально скучно: нет базы, нет секретов
// к хранилищам, нет внешних скриптов на странице, весь HTML рендерится
// на сервере. Всё, что он умеет, — проверить подпись сессии и сходить
// к трём соседям по HTTP.
//
// Это осознанное проектное решение: самый доступный снаружи сервис
// должен быть и самым бедным по возможностям. Захват web-admin даёт
// атакующему доступ к API соседей от имени одного пользователя,
// а не строку подключения к PostgreSQL.
package main

import (
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Ivanvnew75/libs/common"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
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

	logger := common.NewLogger("web-admin", version, cfg.LogFormat, cfg.LogLevel)
	logger.Info("starting", slog.String("commit", commit),
		slog.Bool("secure_cookie", cfg.SecureCookie))

	// Громкое предупреждение вместо тихого флага.
	//
	// SECURE_COOKIE=false — законная настройка для dev и дыра для prod:
	// сессионная кука уйдёт по незашифрованному соединению. Настройка,
	// которую можно включить незаметно, рано или поздно оказывается
	// включённой в проде. Строка WARN в логе делает это видимым
	// и в kubectl logs, и в Loki.
	if !cfg.SecureCookie {
		logger.Warn("СЕССИОННАЯ КУКА БЕЗ ФЛАГА Secure — допустимо только в dev по HTTP")
	}

	ctx, stop := common.SignalContext()
	defer stop()

	clients := NewClients(cfg.AnswersURL, cfg.AnalyticsURL, cfg.RecommenderURL,
		cfg.HTTPTimeout, cfg.HTTPRetries)
	metrics := common.NewMetrics("web-admin")

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(common.RequestID(), common.PropagateRequestID(),
		common.RequestLogger(logger), metrics.Middleware())
	e.Use(securityHeaders())

	// Ограничение частоты запросов.
	//
	// Это ЕДИНСТВЕННЫЙ сервис, доступный из интернета, и у него есть
	// эндпоинт /login, принимающий токен. Без лимита он превращается
	// в стенд для перебора подписей: миллион попыток в минуту стоят
	// атакующему одну строку кода. 20 запросов в секунду с адреса —
	// с запасом для человека и бесполезно для перебора.
	//
	// Лимит стоит и на ingress (задание 13), но дублирование здесь
	// намеренно: прямой доступ к поду изнутри кластера ingress минует.
	e.Use(middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
		Store: middleware.NewRateLimiterMemoryStoreWithConfig(
			middleware.RateLimiterMemoryStoreConfig{Rate: 20, Burst: 40, ExpiresIn: time.Minute},
		),
	}))

	metrics.Register(e)
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	// readiness НЕ проверяет соседей.
	//
	// Соблазн: «проверим, что answers доступен». Тогда падение answers
	// вывело бы web-admin из балансировки, и вместо страницы с частью
	// данных пользователь получил бы 503 от ingress. Каскадный отказ
	// вместо частичной деградации — плохой обмен. Готовность сервиса
	// про НЕГО, а не про его зависимости.
	e.GET("/ready", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ready"})
	})

	// Стили отдельным ресурсом — см. разбор в page.go.
	// Доступны БЕЗ сессии: это публичная статика, и требовать на неё
	// авторизацию значило бы отдавать неоформленную страницу входа.
	e.GET("/style.css", func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "public, max-age=3600")
		return c.Blob(http.StatusOK, "text/css; charset=utf-8", []byte(styleCSS))
	})

	secret := []byte(cfg.LinkSecret)
	e.GET("/login", handleLogin(secret, cfg.SessionTTL, cfg.SecureCookie, logger))
	e.GET("/logout", handleLogout(cfg.SecureCookie))

	// Всё остальное — только с сессией.
	authed := e.Group("", requireSession(secret))
	authed.GET("/", handleIndex(clients, logger))
	authed.GET("/api/answers", func(c echo.Context) error {
		items, err := clients.Answers(c.Request().Context(), userIDFrom(c), 50)
		if err != nil {
			return c.JSON(http.StatusBadGateway, map[string]string{"error": "upstream unavailable"})
		}
		return c.JSON(http.StatusOK, items)
	})
	authed.GET("/api/summary", func(c echo.Context) error {
		sm, err := clients.Summary(c.Request().Context(), userIDFrom(c))
		if err != nil {
			return c.JSON(http.StatusBadGateway, map[string]string{"error": "upstream unavailable"})
		}
		return c.JSON(http.StatusOK, sm)
	})

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

	<-ctx.Done()
	shutdownCtx, cancel := common.ShutdownContext(cfg.ShutdownTimeout)
	defer cancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.String("error", err.Error()))
	}
	wg.Wait()
	logger.Info("stopped")
	_ = os.Stdout.Sync()
}

// handleIndex собирает страницу из трёх источников.
func handleIndex(c1 *Clients, logger *slog.Logger) echo.HandlerFunc {
	return func(c echo.Context) error {
		userID := userIDFrom(c)
		ctx := c.Request().Context()

		// Три запроса параллельно, а не последовательно.
		// Последовательно страница ждала бы сумму трёх задержек;
		// параллельно — максимум из трёх. При таймауте 5 секунд
		// на запрос это разница между 15 и 5 секундами в худшем случае.
		var (
			wg      sync.WaitGroup
			summary Summary
			answers []Answer
			daily   []DailyPoint
			rec     Recommendation
			recErr  bool
		)
		wg.Add(4)
		go func() { defer wg.Done(); summary, _ = c1.Summary(ctx, userID) }()
		go func() { defer wg.Done(); answers, _ = c1.Answers(ctx, userID, 20) }()
		go func() { defer wg.Done(); daily, _ = c1.Daily(ctx, userID, 30) }()
		go func() {
			defer wg.Done()
			var err error
			rec, err = c1.Recommendation(ctx, userID)
			if err != nil {
				// Рекомендация — самая необязательная часть страницы
				// и единственная, зависящая от ВНЕШНЕГО API. Её отказ
				// обязан деградировать до отсутствия блока, а не
				// до пятисотки на весь кабинет.
				recErr = true
				logger.Warn("рекомендация недоступна", slog.String("error", err.Error()))
			}
		}()
		wg.Wait()

		return pageTmpl.Execute(c.Response().Writer, pageData{
			Summary:        summary,
			Answers:        answers,
			Recommendation: rec,
			RecError:       recErr,
			Chart:          renderChart(daily),
		})
	}
}

// securityHeaders выставляет заголовки на каждый ответ.
//
// Ставятся и здесь, и на ingress (задание 13). Дублирование намеренно:
// заголовки на ingress защищают трафик снаружи, но не защищают от
// запроса, пришедшего к поду напрямую изнутри кластера. Каждый уровень
// должен быть безопасен сам по себе — это и называется defense in depth.
func securityHeaders() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			h := c.Response().Header()
			// CSP БЕЗ 'unsafe-inline' вообще — ни для скриптов, ни для стилей.
			//
			// Скриптов на странице нет ни одного, это проверяемое свойство.
			// Инлайновые стили сначала были разрешены с формулировкой
			// «вынос CSS ради одного правила не стоит того». DAST показал,
			// что стоит: правило ZAP 10055. Стили переехали в /style.css,
			// атрибуты style="" стали классами, и из политики ушёл целый
			// класс разрешений. Цена — один дополнительный маршрут.
			h.Set("Content-Security-Policy",
				"default-src 'none'; style-src 'self'; "+
					"img-src 'self' data:; form-action 'self'; "+
					"base-uri 'none'; frame-ancestors 'none'")
			// Запрет вставки страницы в чужой iframe — защита
			// от clickjacking. frame-ancestors в CSP делает то же самое,
			// X-Frame-Options оставлен для старых браузеров.
			h.Set("X-Frame-Options", "DENY")
			h.Set("X-Content-Type-Options", "nosniff")
			// Referrer не утекает наружу: иначе адрес с токеном входа
			// уехал бы в заголовке Referer на любой внешний сайт.
			h.Set("Referrer-Policy", "no-referrer")
			h.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")

			// Изоляция от чужих страниц. Нашёл DAST (правило 90004).
			// COOP разрывает связь window.opener с открывшей нас
			// вкладкой, CORP запрещает встраивать наши ответы
			// в чужие документы. Обе директивы бесплатны и закрывают
			// класс атак через кросс-оконное взаимодействие.
			h.Set("Cross-Origin-Opener-Policy", "same-origin")
			h.Set("Cross-Origin-Resource-Policy", "same-origin")
			// COEP require-corp: страница не загрузит НИ ОДИН чужой
			// ресурс, если тот явно не разрешил себя встраивать.
			// Безопасно ровно потому, что внешних ресурсов на странице
			// нет вообще — на сайте с чужими картинками или шрифтами
			// эта директива всё бы сломала, и включать её пришлось бы
			// вместе с правкой всех источников.
			h.Set("Cross-Origin-Embedder-Policy", "require-corp")

			// Страница содержит данные о самочувствии человека.
			// Без явного запрета она может осесть в кэше браузера
			// и в кэше промежуточного прокси — и остаться доступной
			// следующему, кто сядет за тот же компьютер.
			// Нашёл DAST (правило 10049 «Non-Storable Content»).
			if c.Path() != "/style.css" {
				h.Set("Cache-Control", "no-store")
			}
			return next(c)
		}
	}
}
