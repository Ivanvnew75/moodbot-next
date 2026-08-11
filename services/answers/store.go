package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Ivanvnew75/libs/events"
)

type Answer struct {
	ID         int64     `json:"id"`
	UserID     int64     `json:"user_id"`
	Question   string    `json:"question"`
	Answer     string    `json:"answer"`
	OccurredAt time.Time `json:"occurred_at"`
}

type Store struct{ pool *pgxpool.Pool }

func NewStore(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Save записывает событие. Возвращает inserted=false, если такое
// событие уже было.
//
// ON CONFLICT DO NOTHING — вся защита от дублей at-least-once доставки.
// Обрати внимание: без RETURNING мы бы не отличили «вставили» от
// «уже было», а это разные события для метрик. Дубли — не ошибка,
// но их количество стоит видеть: внезапный рост означает, что
// потребитель не успевает коммитить offset и группа ребалансится.
func (s *Store) Save(ctx context.Context, e events.AnswerReceived) (bool, error) {
	const q = `
		INSERT INTO answers.answers
			(event_id, user_id, telegram_id, question, answer, occurred_at, request_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING id`

	var id int64
	err := s.pool.QueryRow(ctx, q,
		e.EventID, e.UserID, e.TelegramID, e.Question, e.Answer, e.OccurredAt, e.RequestID,
	).Scan(&id)

	if err != nil {
		// pgx возвращает ErrNoRows, когда RETURNING не отдал строк —
		// то есть когда сработал DO NOTHING. Это НЕ ошибка.
		if err.Error() == "no rows in result set" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List отдаёт последние ответы пользователя.
func (s *Store) List(ctx context.Context, userID int64, limit int) ([]Answer, error) {
	const q = `
		SELECT id, user_id, question, answer, occurred_at
		FROM answers.answers
		WHERE user_id = $1
		ORDER BY occurred_at DESC
		LIMIT $2`

	rows, err := s.pool.Query(ctx, q, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Инициализируем пустым срезом, а не nil: json.Marshal(nil) даёт
	// null, а не []. Клиент на фронте, ожидающий массив, на null падает.
	out := []Answer{}
	for rows.Next() {
		var a Answer
		if err := rows.Scan(&a.ID, &a.UserID, &a.Question, &a.Answer, &a.OccurredAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Count — сколько всего ответов у пользователя.
func (s *Store) Count(ctx context.Context, userID int64) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM answers.answers WHERE user_id = $1`, userID).Scan(&n)
	return n, err
}
