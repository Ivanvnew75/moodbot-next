package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Клиент OpenAI-совместимого API.
//
// ПОЧЕМУ «OpenAI-совместимый», А НЕ КОНКРЕТНЫЙ ПРОВАЙДЕР.
// Формат /v1/chat/completions стал де-факто стандартом: его понимают
// OpenAI, российские прокси, Ollama, vLLM, llama.cpp. Один клиент —
// и провайдер меняется переменной окружения, а не переписыванием кода.
// Это ровно Фактор 4: внешний API — такой же подключаемый ресурс,
// как база.
//
// ЧТО ЗДЕСЬ ГЛАВНОЕ С ТОЧКИ ЗРЕНИЯ DEVOPS.
// Не промпт. Главное — то, что этот вызов уходит ЗА ПРЕДЕЛЫ кластера,
// к чужому сервису, который:
//   - может отвечать секундами вместо миллисекунд;
//   - может лежать час;
//   - берёт деньги за каждый запрос;
//   - получает ТЕКСТ ПОЛЬЗОВАТЕЛЯ, то есть персональные данные.
//
// Отсюда всё, что ниже: таймаут, ограничение размера ответа,
// размыкатель цепи, кэш и урезание отправляемых данных.

type LLM struct {
	baseURL string
	apiKey  string
	model   string
	http    *http.Client
	log     *slog.Logger

	breaker *breaker
}

func NewLLM(baseURL, apiKey, model string, timeout time.Duration, log *slog.Logger) *LLM {
	return &LLM{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		http: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				// Пул соединений к одному хосту. Дефолт MaxIdleConnsPerHost=2
				// заставляет открывать новое TLS-соединение почти на каждый
				// запрос — а TLS-рукопожатие к внешнему API это сотни
				// миллисекунд, часто больше самого запроса.
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		log:     log,
		breaker: newBreaker(5, 60*time.Second),
	}
}

type chatRequest struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	MaxTokens int           `json:"max_tokens"`
	// Температура пониже: нам нужна осмысленная рекомендация,
	// а не творчество. Заодно ответы стабильнее между вызовами,
	// что делает поведение сервиса предсказуемее.
	Temperature float64 `json:"temperature"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

var ErrCircuitOpen = errors.New("circuit breaker is open")

// Complete отправляет запрос и возвращает текст ответа.
func (l *LLM) Complete(ctx context.Context, system, user string) (string, error) {
	// Размыкатель проверяется ДО запроса. Смысл именно в этом:
	// когда внешний API лежит, мы не тратим на него таймаут
	// (а значит и время пользователя, и горутину), а сразу
	// отвечаем «недоступно» и отдаём заготовленный текст.
	if !l.breaker.allow() {
		return "", ErrCircuitOpen
	}

	body, err := json.Marshal(chatRequest{
		Model: l.model,
		Messages: []chatMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
		// Потолок длины ответа. Это не только про качество:
		// у большинства провайдеров оплата идёт за токены,
		// и запрос без max_tokens — это открытый счёт.
		MaxTokens:   300,
		Temperature: 0.4,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		l.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+l.apiKey)

	resp, err := l.http.Do(req)
	if err != nil {
		l.breaker.failure()
		return "", fmt.Errorf("llm request: %w", err)
	}
	defer resp.Body.Close()

	// Ограничение размера ответа. Без него ответ чужого сервиса
	// (или подменённого посредника) на гигабайт мусора съедает
	// память пода до OOMKilled. io.LimitReader стоит одну строку
	// и закрывает целый класс отказов.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		l.breaker.failure()
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		// 429 и 5xx — размыкаем. 4xx (кроме 429) означают нашу ошибку:
		// неверный ключ, несуществующая модель. Размыкать цепь из-за
		// собственной ошибки конфигурации бессмысленно — она не пройдёт
		// и через минуту, а лог должен показывать её каждый раз.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			l.breaker.failure()
		}
		// Тело ошибки обрезаем в логе: провайдеры иногда возвращают
		// в нём эхо запроса, а в запросе — текст пользователя.
		return "", fmt.Errorf("llm status %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}

	var out chatResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		l.breaker.failure()
		return "", fmt.Errorf("llm decode: %w", err)
	}
	if out.Error != nil {
		return "", fmt.Errorf("llm error: %s", out.Error.Message)
	}
	if len(out.Choices) == 0 {
		return "", errors.New("llm returned no choices")
	}

	l.breaker.success()
	return strings.TrimSpace(out.Choices[0].Message.Content), nil
}

// ── Размыкатель цепи ────────────────────────────────────────────────
//
// Простейший вариант: N ошибок подряд → цепь разомкнута на T секунд,
// затем один пробный запрос.
//
// Зачем он здесь, если есть таймаут. Таймаут ограничивает ОДИН запрос.
// Когда внешний API лежит, каждый посетитель кабинета всё равно ждёт
// полный таймаут, и при таймауте 8 секунд десять посетителей займут
// десять горутин на восемь секунд каждая. Размыкатель превращает
// «медленно и с ошибкой» в «быстро и с ошибкой» — а быстрая ошибка
// даёт возможность корректно деградировать.
type breaker struct {
	mu sync.Mutex

	threshold int
	cooldown  time.Duration

	failures int
	openedAt time.Time
}

func newBreaker(threshold int, cooldown time.Duration) *breaker {
	return &breaker{threshold: threshold, cooldown: cooldown}
}

func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.threshold {
		return true
	}
	// Полуоткрытое состояние: по истечении паузы пропускаем ОДИН
	// пробный запрос. Без него цепь пришлось бы закрывать вручную,
	// а сервис не восстановился бы сам после починки провайдера.
	if time.Since(b.openedAt) > b.cooldown {
		b.failures = b.threshold - 1
		return true
	}
	return false
}

func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.failures == b.threshold {
		b.openedAt = time.Now()
	}
}

func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
}

func (b *breaker) state() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures >= b.threshold && time.Since(b.openedAt) <= b.cooldown {
		return "open"
	}
	return "closed"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
