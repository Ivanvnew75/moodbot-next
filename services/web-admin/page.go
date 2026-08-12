package main

import (
	"fmt"
	"html/template"
	"strings"
)

// Страница кабинета собирается на сервере, из html/template.
//
// ПОЧЕМУ НЕ SPA НА REACT.
// Кабинет показывает список и график. SPA принесла бы сборку фронтенда
// в CI, отдельный артефакт, CORS между доменами и — главное для этого
// курса — целый пласт клиентских уязвимостей. Серверный рендер
// с html/template даёт экранирование ПО УМОЛЧАНИЮ и по контексту:
// один и тот же текст экранируется по-разному внутри HTML, внутри
// атрибута и внутри JavaScript. Это ровно та защита от XSS, которую
// в SPA приходится выстраивать руками.
//
// График рисуется инлайновым SVG, а не библиотекой из CDN.
// Причина не в размере: любой внешний скрипт — это доверие к чужому
// домену на странице с личными данными, и он же ломает строгий CSP
// (см. заголовки в main.go). Ни одного внешнего ресурса на странице
// нет вообще — это проверяемое свойство, а не намерение.

var pageTmpl = template.Must(template.New("page").Parse(`
<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MoodBot — мой кабинет</title>
<link rel="stylesheet" href="/style.css">
</head>
<body>
  <h1>Мой кабинет</h1>
  <p class="muted">История ответов и динамика настроения. Первый ответ: {{.Summary.FirstSeen}}</p>

  <div class="cards">
    <div class="card"><div class="muted">Всего ответов</div><div class="v">{{.Summary.Total}}</div></div>
    <div class="card"><div class="muted">Среднее настроение</div><div class="v">{{printf "%.2f" .Summary.AvgScore}}</div></div>
    <div class="card"><div class="muted">За 7 дней</div><div class="v">{{printf "%.2f" .Summary.Last7}}</div></div>
    <div class="card"><div class="muted">Тенденция</div><div class="v">{{.Summary.Trend}}</div></div>
  </div>

  {{if .Recommendation.Text}}
  <div class="rec">
    <strong>Рекомендация</strong><br>
    {{.Recommendation.Text}}
    <div class="muted">источник: {{.Recommendation.Source}}</div>
  </div>
  {{else if .RecError}}
  <p class="err">Рекомендации сейчас недоступны — остальное работает.</p>
  {{end}}

  <h2 class="sec">Динамика</h2>
  {{if .Chart}}{{.Chart}}{{else}}<p class="muted">Данных для графика пока мало.</p>{{end}}

  <h2 class="sec">Последние ответы</h2>
  {{if .Answers}}
  <table>
    <tr><th>Когда</th><th>Вопрос</th><th>Ответ</th></tr>
    {{range .Answers}}
    <tr>
      <td class="muted">{{.OccurredAt.Format "02.01 15:04"}}</td>
      <td class="muted">{{.Question}}</td>
      <td>{{.Answer}}</td>
    </tr>
    {{end}}
  </table>
  {{else}}
  <p class="muted">Ответов пока нет. Напишите боту, как вы себя чувствуете.</p>
  {{end}}

  <footer>
    <a href="/logout">Выйти</a> · данные видны только вам
  </footer>
</body>
</html>
`))

// styleCSS — стили ОТДЕЛЬНЫМ ресурсом, а не тегом <style>.
//
// Причина не косметическая. Инлайновые стили требуют в CSP директивы
// style-src 'unsafe-inline', а она разрешает браузеру исполнять ЛЮБОЙ
// стиль со страницы — включая внедрённый атакующим. Через CSS можно
// вытащить данные (селекторы по значению атрибута + background-image
// с адресом атакующего) и подделать интерфейс поверх настоящего.
//
// Нашёл DAST (ZAP, правило 10055 «CSP: style-src unsafe-inline»).
// Формально это предупреждение, а не уязвимость, но убирается оно
// дёшево: один дополнительный маршрут — и из CSP уходит целый класс
// разрешений. Заодно пришлось убрать атрибуты style="" из разметки:
// они блокируются той же директивой.
const styleCSS = `
  :root { color-scheme: light dark; }
  body { font-family: system-ui, sans-serif; max-width: 780px; margin: 0 auto;
         padding: 24px 16px 64px; line-height: 1.5; }
  h1 { font-size: 1.4rem; margin-bottom: 4px; }
  .muted { opacity: .7; font-size: .9rem; }
  .cards { display: flex; flex-wrap: wrap; gap: 12px; margin: 20px 0; }
  .card { flex: 1 1 150px; border: 1px solid rgba(128,128,128,.35);
          border-radius: 10px; padding: 12px 14px; }
  .card .v { font-size: 1.6rem; font-weight: 600; }
  .rec { border-left: 3px solid #6c8ebf; padding: 10px 14px; margin: 20px 0;
         background: rgba(128,128,128,.08); border-radius: 0 8px 8px 0; }
  table { border-collapse: collapse; width: 100%; margin-top: 12px; }
  td, th { text-align: left; padding: 6px 8px; border-bottom: 1px solid rgba(128,128,128,.25);
           vertical-align: top; }
  th { font-weight: 600; font-size: .85rem; opacity: .8; }
  svg { max-width: 100%; height: auto; }
  .err { color: #b23; font-size: .9rem; }
  footer { margin-top: 40px; font-size: .85rem; opacity: .7; }
  a { color: inherit; }
  .sec { font-size: 1.1rem; }
`

type pageData struct {
	Summary        Summary
	Answers        []Answer
	Recommendation Recommendation
	RecError       bool
	Chart          template.HTML
}

// renderChart рисует линейный график средней оценки по дням.
//
// template.HTML означает «этот HTML не экранировать», и это опасный тип:
// всё, что в него попадает, вставляется в страницу как есть. Поэтому
// SVG собирается ЗДЕСЬ из чисел, посчитанных на сервере, и ни один
// пользовательский текст сюда не попадает. Единственные строки —
// даты формата YYYY-MM-DD из ClickHouse, и они дополнительно
// прогоняются через экранирование.
func renderChart(points []DailyPoint) template.HTML {
	if len(points) < 2 {
		return ""
	}

	const w, h, pad = 700.0, 180.0, 28.0
	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img" aria-label="Динамика настроения">`, w, h)

	// Шкала жёстко -2..2: это диапазон mood_score. Автомасштаб по данным
	// был бы красивее, но врал бы — колебание от 0.9 до 1.0 выглядело бы
	// как размах во весь экран.
	y := func(v float64) float64 { return pad + (2-v)/4*(h-2*pad) }
	x := func(i int) float64 { return pad + float64(i)/float64(len(points)-1)*(w-2*pad) }

	// Ось нуля — ориентир «нейтральное настроение».
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="currentColor" stroke-opacity=".25"/>`,
		pad, y(0), w-pad, y(0))

	var path strings.Builder
	for i, p := range points {
		if i == 0 {
			fmt.Fprintf(&path, "M %.1f %.1f", x(i), y(p.AvgScore))
		} else {
			fmt.Fprintf(&path, " L %.1f %.1f", x(i), y(p.AvgScore))
		}
	}
	fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="#6c8ebf" stroke-width="2"/>`, path.String())

	for i, p := range points {
		fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3" fill="#6c8ebf"><title>%s: %.2f</title></circle>`,
			x(i), y(p.AvgScore), template.HTMLEscapeString(p.Date), p.AvgScore)
	}

	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="11" fill="currentColor" fill-opacity=".6">%s</text>`,
		pad, h-6, template.HTMLEscapeString(points[0].Date))
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="11" text-anchor="end" fill="currentColor" fill-opacity=".6">%s</text>`,
		w-pad, h-6, template.HTMLEscapeString(points[len(points)-1].Date))
	b.WriteString(`</svg>`)

	// #nosec G203 -- в эту строку не попадает пользовательский ввод.
	//
	// gosec прав в общем случае: template.HTML отключает экранирование,
	// и это опасный тип. Здесь SVG собирается из чисел, посчитанных
	// на сервере (координаты, средние оценки), а единственные строки —
	// даты формата YYYY-MM-DD из ClickHouse — дополнительно проходят
	// через template.HTMLEscapeString выше.
	//
	// Инвариант, который нужно удержать при правках: если сюда
	// когда-нибудь попадёт текст ответа пользователя, подавление
	// обязано быть снято, а SVG — собран через шаблон.
	return template.HTML(b.String())
}

// pageMessage — простая страница для ошибок входа.
// Тексты здесь ЗАДАНЫ В КОДЕ и никогда не приходят от пользователя,
// поэтому конкатенация безопасна. Как только сюда захочется подставить
// что-то из запроса — надо переходить на template.
func pageMessage(title, text string) string {
	return `<!doctype html><html lang="ru"><head><meta charset="utf-8">` +
		`<meta name="viewport" content="width=device-width, initial-scale=1">` +
		`<title>` + template.HTMLEscapeString(title) + `</title>` +
		`<link rel="stylesheet" href="/style.css">` +
		`</head><body><h1 class="msg">` + template.HTMLEscapeString(title) + `</h1>` +
		`<p>` + template.HTMLEscapeString(text) + `</p></body></html>`
}
