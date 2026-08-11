package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DDL ClickHouse держим в коде, а не в .sql-файлах с версиями, как
// в PostgreSQL. Причина не в лени: в ClickHouse нет транзакционного DDL,
// поэтому «применить миграцию атомарно» невозможно в принципе, и весь
// механизм учёта версий свёлся бы к самообману. Вместо этого — идемпотентный
// DDL с IF NOT EXISTS, который безопасно выполнять при каждом старте.
//
// Оборотная сторона честная: изменение типа колонки придётся делать
// руками через ALTER, и это надо помнить. Для схемы из девяти полей
// приемлемо; для сложной схемы взяли бы отдельный инструмент.
var ddl = []string{
	`CREATE DATABASE IF NOT EXISTS moodbot`,

	`CREATE TABLE IF NOT EXISTS moodbot.answers
	(
	    event_id    String,
	    user_id     Int64,
	    telegram_id Int64,
	    question    String,
	    answer      String,
	    -- mood_score: грубая оценка настроения по тексту, -2..2.
	    -- Int8, а не Float: значение дискретное, и хранить его во float
	    -- значило бы получать 0.30000000000000004 в агрегатах.
	    mood_score  Int8,
	    occurred_at DateTime,
	    inserted_at DateTime DEFAULT now(),
	    request_id  String
	)
	ENGINE = ReplacingMergeTree(inserted_at)
	-- ORDER BY определяет и сортировку данных на диске, и ключ
	-- схлопывания дублей у ReplacingMergeTree. event_id в конце ключа —
	-- именно ради дедупликации: два одинаковых события схлопнутся
	-- при слиянии частей.
	--
	-- ВАЖНАЯ ОГОВОРКА, ПРО КОТОРУЮ ЗАБЫВАЮТ:
	-- ReplacingMergeTree убирает дубли ТОЛЬКО при слиянии, то есть
	-- когда-нибудь, и только внутри одной партиции. Запрос сразу после
	-- вставки увидит оба экземпляра. Поэтому в аналитических запросах
	-- ниже используется agg-функция по event_id, а не наивный count().
	-- Полагаться на ReplacingMergeTree как на «UNIQUE» нельзя.
	ORDER BY (user_id, occurred_at, event_id)
	-- Партиционирование по месяцу: удаление старых данных превращается
	-- в мгновенный DROP PARTITION вместо тяжёлого DELETE, а запросы
	-- за последний месяц читают одну партицию вместо всей таблицы.
	PARTITION BY toYYYYMM(occurred_at)
	-- TTL: год хранения. У производного хранилища ретеншен должен быть
	-- явным — иначе диск кончится молча, а восстановить всё равно
	-- можно переигрыванием Kafka.
	TTL occurred_at + INTERVAL 1 YEAR`,
}

func applySchema(ctx context.Context, conn driver.Conn, log *slog.Logger) error {
	for i, q := range ddl {
		if err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("ddl[%d]: %w", i, err)
		}
	}
	log.Info("схема ClickHouse применена", slog.Int("statements", len(ddl)))
	return nil
}
