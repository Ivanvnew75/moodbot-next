package main

import "github.com/prometheus/client_golang/prometheus"

// Метрики потребителя. Отдельно от answers: у сервисов разные consumer
// group, и их лаги надо видеть по отдельности — расхождение лагов
// сразу показывает, какой из двух потребителей отстаёт и почему.
type appMetrics struct {
	lag      prometheus.Gauge
	inserted prometheus.Counter
	failed   prometheus.Counter
}

func newAppMetrics(reg *prometheus.Registry) *appMetrics {
	m := &appMetrics{
		lag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "analytics_consumer_lag",
			Help: "Отставание consumer group analytics-writer от конца топика",
		}),
		inserted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "analytics_inserted_total",
			Help: "Событий записано в ClickHouse",
		}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "analytics_failed_total",
			Help: "Ошибок обработки сообщений",
		}),
	}
	reg.MustRegister(m.lag, m.inserted, m.failed)
	return m
}
