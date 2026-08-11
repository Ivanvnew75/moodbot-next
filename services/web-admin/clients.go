package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Ivanvnew75/libs/common"
)

// Клиенты соседних сервисов.
//
// web-admin — сервис-агрегатор: своих данных у него нет вообще,
// он собирает ответ из трёх источников. Отсюда два следствия,
// которые видны в коде ниже:
//
//  1. У него нет ни базы, ни секретов к базам. Компрометация
//     web-admin — самого уязвимого сервиса, потому что он публичный, —
//     не даёт прямого доступа к данным, только к API соседей.
//     Это осознанное решение, а не случайность.
//
//  2. Отказ ЛЮБОГО соседа не должен ронять страницу целиком.
//     Кабинет без рекомендации полезнее, чем пустая страница
//     с пятисоткой.

type Answer struct {
	ID         int64     `json:"id"`
	Question   string    `json:"question"`
	Answer     string    `json:"answer"`
	OccurredAt time.Time `json:"occurred_at"`
}

type DailyPoint struct {
	Date     string  `json:"date"`
	Count    uint64  `json:"count"`
	AvgScore float64 `json:"avg_score"`
}

type Summary struct {
	Total     uint64  `json:"total"`
	AvgScore  float64 `json:"avg_score"`
	Last7     float64 `json:"avg_last_7d"`
	Prev7     float64 `json:"avg_prev_7d"`
	Trend     string  `json:"trend"`
	FirstSeen string  `json:"first_seen"`
}

type Recommendation struct {
	Text   string `json:"text"`
	Source string `json:"source"`
}

type Clients struct {
	answers     string
	analytics   string
	recommender string
	http        *common.Client
}

func NewClients(answers, analytics, recommender string, timeout time.Duration, retries int) *Clients {
	return &Clients{
		answers:     strings.TrimRight(answers, "/"),
		analytics:   strings.TrimRight(analytics, "/"),
		recommender: strings.TrimRight(recommender, "/"),
		// Общий клиент с таймаутом и ретраями из libs. Таймаут здесь
		// не «на всякий случай»: без него зависший сосед держит
		// горутину и соединение web-admin до бесконечности, и хватает
		// нескольких таких запросов, чтобы кабинет перестал отвечать
		// всем. Это самый дешёвый способ положить сервис-агрегатор.
		http: common.NewClient(timeout, retries),
	}
}

func (c *Clients) Answers(ctx context.Context, userID int64, limit int) ([]Answer, error) {
	var out []Answer
	url := fmt.Sprintf("%s/answers?user_id=%d&limit=%d", c.answers, userID, limit)
	return out, c.http.DoJSON(ctx, http.MethodGet, url, nil, &out)
}

func (c *Clients) Summary(ctx context.Context, userID int64) (Summary, error) {
	var out Summary
	url := fmt.Sprintf("%s/analytics/summary?user_id=%d", c.analytics, userID)
	return out, c.http.DoJSON(ctx, http.MethodGet, url, nil, &out)
}

func (c *Clients) Daily(ctx context.Context, userID int64, days int) ([]DailyPoint, error) {
	var out []DailyPoint
	url := fmt.Sprintf("%s/analytics/daily?user_id=%d&days=%d", c.analytics, userID, days)
	return out, c.http.DoJSON(ctx, http.MethodGet, url, nil, &out)
}

func (c *Clients) Recommendation(ctx context.Context, userID int64) (Recommendation, error) {
	var out Recommendation
	url := fmt.Sprintf("%s/recommend?user_id=%d", c.recommender, userID)
	return out, c.http.DoJSON(ctx, http.MethodGet, url, nil, &out)
}
