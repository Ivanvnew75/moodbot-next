// Package pgmigrate — минимальный накатчик SQL-миграций для PostgreSQL.
//
// Почему не golang-migrate/goose: сервису нужно ровно три вещи —
// встроить .sql в бинарник, применить по порядку, не дать двум процессам
// сделать это одновременно. Внешняя зависимость с CLI, драйверами и
// собственным форматом версий здесь дороже, чем 100 строк. Подход
// повторяет migrate.go из сервиса users (курс 12-factor-app), только
// вынесен в общий пакет — теперь его используют несколько сервисов.
package pgmigrate

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Run применяет все непринятые миграции из fsys (файлы *.up.sql).
//
// schema — схема, которой владеет сервис. В неё же кладётся таблица
// учёта schema_migrations.
//
// ПОЧЕМУ УЧЁТ ВЕРСИЙ ЖИВЁТ В СХЕМЕ СЕРВИСА, А НЕ В public.
// Наступил на это на живом стенде: первая версия клала schema_migrations
// в public, и миграция упала с «permission denied for schema public» —
// ровно потому, что роль answers намеренно лишена права создавать
// объекты в public (см. scripts/bootstrap-answers-db.sh).
//
// Ошибка оказалась полезной: она вскрыла более глубокую проблему.
// Если несколько сервисов делят одну базу, общая таблица
// public.schema_migrations СТОЛКНЁТ их версии — у каждого сервиса
// своя миграция «0001_init», и второй сервис решил бы, что она
// уже применена. Учёт версий обязан быть внутри схемы владельца.
//
// lockID — произвольное, но зафиксированное для сервиса число.
// У каждого сервиса оно должно быть СВОЁ: advisory lock глобален
// на всю базу, и общий id заставил бы миграции разных сервисов
// ждать друг друга без всякой на то причины.
func Run(ctx context.Context, dsn, schema string, fsys fs.FS, dir string, lockID int64, log *slog.Logger) error {
	// Одиночное соединение, а не пул: advisory lock живёт в пределах
	// СЕССИИ. Через пул следующий запрос может уйти в другое соединение,
	// и блокировка окажется взята не там, где выполняется миграция.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	// Блокировка берётся ДО создания таблицы версий — иначе два процесса
	// могли бы одновременно создать её и одновременно решить,
	// что применённых миграций нет.
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", lockID)

	// Схема создаётся здесь, а не первой миграцией: таблица учёта версий
	// должна лежать в ней, а значит схема нужна ДО первой миграции.
	// Имя схемы подставляется форматированием, а не параметром запроса:
	// параметры ($1) в PostgreSQL работают только для значений, но не для
	// идентификаторов. Отсюда обязанность вызывающего кода — не пускать
	// сюда пользовательский ввод; здесь имя схемы задано константой в коде.
	if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgIdent(schema)); err != nil {
		return fmt.Errorf("create schema %s: %w", schema, err)
	}

	versionTable := pgIdent(schema) + ".schema_migrations"
	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+versionTable+` (
			version    TEXT        PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := conn.Query(ctx, "SELECT version FROM "+versionTable)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".up.sql") {
			names = append(names, e.Name())
		}
	}
	// Сортировка по имени, а не порядок из ReadDir: порядок применения
	// миграций обязан быть детерминированным на любой файловой системе.
	sort.Strings(names)

	for _, name := range names {
		version := strings.TrimSuffix(name, ".up.sql")
		if applied[version] {
			continue
		}

		body, err := fs.ReadFile(fsys, dir+"/"+name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}

		log.Info("применяю миграцию", slog.String("version", version))

		// КАЖДАЯ миграция — в своей транзакции, вместе с отметкой в
		// schema_migrations. Иначе возможен разрыв: SQL применился,
		// процесс упал до записи версии — и повторный запуск попытается
		// применить её снова. В PostgreSQL DDL транзакционен, этим
		// и пользуемся (в MySQL так не вышло бы).
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO "+versionTable+" (version) VALUES ($1)", version); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s: %w", version, err)
		}
	}

	log.Info("миграции применены", slog.Int("total", len(names)))
	return nil
}

// pgIdent экранирует идентификатор для подстановки в SQL.
//
// Нужен потому, что имя схемы нельзя передать параметром запроса.
// Двойные кавычки внутри имени удваиваются — иначе имя вида a"b
// разорвало бы запрос. Для имён из кода это перестраховка,
// но такие перестраховки стоят один вызов, а их отсутствие —
// инцидент с SQL-инъекцией, когда имя однажды придёт снаружи.
func pgIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
