package main

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/Ivanvnew75/libs/authlink"
	"github.com/labstack/echo/v4"
)

const sessionCookie = "moodbot_session"

// ctxUserID — ключ, под которым проверенный user_id кладётся в контекст.
//
// ПРИНЦИП, РАДИ КОТОРОГО ВСЁ ЭТО НАПИСАНО:
// user_id берётся ТОЛЬКО отсюда и НИКОГДА из query-параметра или тела
// запроса. Как только появляется хоть один обработчик, читающий
// ?user_id=, вся аутентификация становится декорацией — атакующему
// достаточно найти этот один обработчик.
const ctxUserID = "user_id"

// handleLogin принимает подписанную ссылку от бота и выдаёт сессию.
func handleLogin(secret []byte, sessionTTL time.Duration, secureCookie bool, log *slog.Logger) echo.HandlerFunc {
	return func(c echo.Context) error {
		userID, err := authlink.Verify(secret, authlink.PurposeLogin, c.QueryParam("token"))
		if err != nil {
			// Причина отказа НЕ раскрывается пользователю: «просрочен»
			// и «подделан» — разные подсказки для того, кто подбирает.
			// В лог пишем подробность, наружу — общий текст.
			log.Warn("отказ во входе", slog.String("error", err.Error()))
			return c.HTML(http.StatusUnauthorized, pageMessage(
				"Ссылка недействительна",
				"Попросите бота выдать новую: отправьте /kabinet в чат."))
		}

		c.SetCookie(&http.Cookie{
			Name:  sessionCookie,
			Value: authlink.Sign(secret, authlink.PurposeSession, userID, sessionTTL),
			Path:  "/",
			// HttpOnly: кука недоступна из JavaScript. Это то, что
			// превращает найденный XSS из «угона сессии» в «испорченную
			// вёрстку» — главная причина, по которой флаг существует.
			HttpOnly: true,
			// Secure: кука не уйдёт по HTTP. Выключается только для
			// локальной разработки — потому и вынесено в конфиг,
			// а не захардкожено.
			Secure: secureCookie,
			// SameSite=Lax: кука не отправляется при межсайтовых POST,
			// то есть CSRF на изменяющие запросы закрыт без отдельных
			// токенов. Strict сломал бы сам сценарий входа —
			// переход по ссылке из Telegram считается межсайтовым.
			SameSite: http.SameSiteLaxMode,
			MaxAge:   int(sessionTTL.Seconds()),
		})

		log.Info("вход выполнен", slog.Int64("user_id", userID))
		// Редирект на корень, чтобы токен ушёл из адресной строки:
		// иначе он останется в истории браузера, в логах прокси
		// и в заголовке Referer при переходе по внешней ссылке.
		return c.Redirect(http.StatusSeeOther, "/")
	}
}

// requireSession — middleware, пускающий дальше только с валидной сессией.
func requireSession(secret []byte) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			cookie, err := c.Cookie(sessionCookie)
			if err != nil {
				return unauthorized(c)
			}
			userID, err := authlink.Verify(secret, authlink.PurposeSession, cookie.Value)
			if err != nil {
				return unauthorized(c)
			}
			c.Set(ctxUserID, userID)
			return next(c)
		}
	}
}

func unauthorized(c echo.Context) error {
	// Для API отдаём JSON, для страниц — HTML. Определяем по префиксу
	// пути, а не по Accept: заголовок подконтролен клиенту.
	if len(c.Path()) >= 5 && c.Path()[:5] == "/api/" {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
	}
	return c.HTML(http.StatusUnauthorized, pageMessage(
		"Нужно войти",
		"Отправьте боту команду /kabinet — он пришлёт ссылку для входа."))
}

func handleLogout(secureCookie bool) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.SetCookie(&http.Cookie{
			Name: sessionCookie, Value: "", Path: "/",
			HttpOnly: true, Secure: secureCookie,
			SameSite: http.SameSiteLaxMode, MaxAge: -1,
		})
		return c.Redirect(http.StatusSeeOther, "/")
	}
}

func userIDFrom(c echo.Context) int64 {
	v, _ := c.Get(ctxUserID).(int64)
	return v
}
