package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Ivanvnew75/libs/events"
)

type Store struct{ conn driver.Conn }

func NewStore(ctx context.Context, addr, database, user, password string, useTLS bool) (*Store, error) {
	opts := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: database, Username: user, Password: password},

		// Сжатие включено: аналитические выборки отдают много данных,
		// и LZ4 почти бесплатен по CPU относительно экономии сети.
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},

		DialTimeout: 5 * time.Second,
		// Пул маленький: ClickHouse не любит много одновременных соединений,
		// у него дорогая обработка запроса, а не соединения. 4 на реплику —
		// с запасом при max_concurrent_queries=16 на сервере.
		MaxOpenConns:    4,
		MaxIdleConns:    2,
		ConnMaxLifetime: 10 * time.Minute,

		Settings: clickhouse.Settings{
			// ── ГЛАВНАЯ НАСТРОЙКА ЭТОГО СЕРВИСА ──────────────────────
			// async_insert переносит батчинг на сторону сервера.
			//
			// Зачем. В MergeTree КАЖДЫЙ INSERT создаёт отдельную «часть»
			// (part) на диске. Вставка по одной строке — это тысячи
			// мелких частей, шторм слияний и в итоге отказ
			// «Too many parts». Это САМАЯ частая ошибка при первом
			// использовании ClickHouse.
			//
			// Классическое лекарство — батчить на стороне клиента.
			// Но у потребителя Kafka батч конфликтует с коммитом offset:
			// либо коммитим до записи (теряем данные при падении),
			// либо держим несколько сообщений незакоммиченными и
			// усложняем обработку ошибок.
			//
			// async_insert решает это без компромисса: сервер копит
			// строки из РАЗНЫХ соединений и пишет их одной частью.
			"async_insert": 1,
			// wait_for_async_insert=1 — INSERT возвращается только после
			// того, как буфер реально записан. Это принципиально: с 0
			// мы бы коммитили offset Kafka, не имея гарантии записи,
			// то есть тихо теряли данные при перезапуске ClickHouse.
			// Скорость ради потери данных — плохой обмен.
			"wait_for_async_insert":        1,
			"async_insert_busy_timeout_ms": 1000,
		},
	}
	if useTLS {
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Store{conn: conn}, nil
}

func (s *Store) Conn() driver.Conn              { return s.conn }
func (s *Store) Close() error                   { return s.conn.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }

// Insert записывает одно событие.
func (s *Store) Insert(ctx context.Context, e events.AnswerReceived) error {
	const q = `INSERT INTO moodbot.answers
		(event_id, user_id, telegram_id, question, answer, mood_score, occurred_at, request_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	return s.conn.Exec(ctx, q,
		e.EventID, e.UserID, e.TelegramID, e.Question, e.Answer,
		MoodScore(e.Answer), e.OccurredAt, e.RequestID)
}

// DailyPoint — точка на графике динамики настроения.
type DailyPoint struct {
	Date     string  `json:"date"`
	Count    uint64  `json:"count"`
	AvgScore float64 `json:"avg_score"`
}

// Daily — динамика по дням.
//
// ДЕДУПЛИКАЦИЯ ДЕЛАЕТСЯ ОДИН РАЗ, В ПОДЗАПРОСЕ, И ЭТО ВАЖНО.
//
// ReplacingMergeTree схлопывает дубли только при слиянии частей,
// то есть «когда-нибудь». До слияния запрос видит обе копии.
//
// Первая версия этого кода заменила count() на uniqExact(event_id)
// и на том успокоилась. Проверка на данных показала, что этого мало:
// количество стало правильным (3), а СРЕДНЕЕ осталось кривым —
// avg(mood_score) по-прежнему усреднял четыре строки вместо трёх
// и выдавал 0.5 вместо 0.67. Починили один агрегат, забыли остальные.
//
// Отсюда правило: дедуплицировать надо СТРОКИ, а не каждый агрегат
// по отдельности. GROUP BY event_id в подзапросе делает это один раз,
// и все агрегаты снаружи автоматически считают по уникальным событиям.
//
// Альтернатива — FINAL: короче, но заставляет ClickHouse сливать части
// на лету при каждом запросе. Правильно и медленно.
//
// ВТОРАЯ ЛОВУШКА, УЖЕ ЧИСТО КЛИКХАУСОВАЯ: псевдонимы в подзапросе
// названы ts и score, а не occurred_at и mood_score. ClickHouse
// подставляет псевдоним обратно в WHERE того же запроса, поэтому
// "any(occurred_at) AS occurred_at" превращает условие
// "WHERE occurred_at >= ..." в агрегатную функцию внутри WHERE,
// и запрос падает с "Aggregate function any(occurred_at) is found
// in WHERE". Сообщение выглядит бессмысленным, пока не знаешь
// про подстановку псевдонимов. Правило: не называй псевдоним
// так же, как колонку, которая участвует в WHERE.
func (s *Store) Daily(ctx context.Context, userID int64, days int) ([]DailyPoint, error) {
	const q = `
		SELECT toString(toDate(ts))                 AS d,
		       count()                              AS cnt,
		       round(ifNotFinite(avg(score), 0), 2) AS avg_score
		FROM (
		    -- Псевдонимы НАМЕРЕННО отличаются от имён колонок (ts, score),
		    -- см. комментарий к функции.
		    SELECT event_id,
		           any(mood_score)  AS score,
		           any(occurred_at) AS ts
		    FROM moodbot.answers
		    WHERE user_id = ? AND occurred_at >= now() - INTERVAL ? DAY
		    GROUP BY event_id
		)
		GROUP BY d
		ORDER BY d`

	rows, err := s.conn.Query(ctx, q, userID, days)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []DailyPoint{}
	for rows.Next() {
		var p DailyPoint
		if err := rows.Scan(&p.Date, &p.Count, &p.AvgScore); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Summary — сводка по пользователю.
type Summary struct {
	Total     uint64  `json:"total"`
	AvgScore  float64 `json:"avg_score"`
	Last7     float64 `json:"avg_last_7d"`
	Prev7     float64 `json:"avg_prev_7d"`
	Trend     string  `json:"trend"`
	FirstSeen string  `json:"first_seen"`
}

// SummaryFor считает сводку одним запросом.
//
// Один запрос с условными агрегатами вместо трёх последовательных:
// ClickHouse читает колонку один раз. Три запроса означали бы
// три прохода по тем же данным — на больших объёмах разница в разы.
//
// ifNotFinite ВОКРУГ КАЖДОГО СРЕДНЕГО — не украшение.
// Наступил на живом стенде: avgIf по пустому окну (у нового пользователя
// нет данных за позапрошлую неделю) возвращает NaN. NaN спокойно
// доезжает до Go, а вот encoding/json его сериализовать НЕ УМЕЕТ —
// и запрос падает с 500 «json: unsupported value: NaN».
//
// Ошибка вылезла в трёх уровнях от причины: симптом — сериализация
// ответа, причина — агрегат по пустому множеству в SQL. Искать
// такое по тексту ошибки бесполезно; помогает вопрос «а что вернёт
// этот запрос, если данных нет вообще?» — его стоит задавать
// каждому агрегату.
func (s *Store) SummaryFor(ctx context.Context, userID int64) (Summary, error) {
	const q = `
		SELECT count()                                                        AS total,
		       round(ifNotFinite(avg(score), 0), 2)                           AS avg_all,
		       round(ifNotFinite(avgIf(score,
		                               ts >= now() - INTERVAL 7 DAY), 0), 2)   AS avg_7,
		       round(ifNotFinite(avgIf(score,
		                               ts >= now() - INTERVAL 14 DAY
		                           AND ts <  now() - INTERVAL 7 DAY), 0), 2)   AS avg_prev_7,
		       toString(toDate(min(ts)))                                      AS first_seen
		FROM (
		    SELECT event_id,
		           any(mood_score)  AS score,
		           any(occurred_at) AS ts
		    FROM moodbot.answers
		    WHERE user_id = ?
		    GROUP BY event_id
		)`

	var sm Summary
	row := s.conn.QueryRow(ctx, q, userID)
	if err := row.Scan(&sm.Total, &sm.AvgScore, &sm.Last7, &sm.Prev7, &sm.FirstSeen); err != nil {
		return sm, err
	}

	switch {
	case sm.Total == 0:
		sm.Trend = "нет данных"
	case sm.Last7 > sm.Prev7+0.3:
		sm.Trend = "лучше"
	case sm.Last7 < sm.Prev7-0.3:
		sm.Trend = "хуже"
	default:
		// Порог 0.3 намеренно не нулевой: без него любое дрожание
		// среднего на сотые доли объявлялось бы «тенденцией».
		// Показывать человеку «вам стало хуже» из-за шума — вредно.
		sm.Trend = "без изменений"
	}
	return sm, nil
}
