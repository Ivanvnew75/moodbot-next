package main

import "github.com/prometheus/client_golang/prometheus"

// Метрики сервиса сверх стандартных HTTP-метрик из libs/common.
//
// Что именно измеряем и почему — важнее, чем сам факт наличия /metrics:
//
//	lag        — отставание от конца топика. Единственная метрика,
//	             по которой видно «сервис жив, но не справляется».
//	saved      — полезная работа. Если lag=0 и saved не растёт,
//	             значит просто нет входящих событий, а не поломка.
//	duplicates — повторы. Норма — единицы; всплеск означает
//	             ребалансы группы или проблемы с коммитом offset.
//	failed     — ошибки обработки. Рост вместе с DLQ = авария.
type appMetrics struct {
	lag        prometheus.Gauge
	saved      prometheus.Counter
	duplicates prometheus.Counter
	failed     prometheus.Counter
}

func newAppMetrics(reg *prometheus.Registry) *appMetrics {
	m := &appMetrics{
		lag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "answers_consumer_lag",
			Help: "Отставание consumer group от конца топика, сообщений",
		}),
		saved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "answers_saved_total",
			Help: "Сохранённых ответов",
		}),
		duplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "answers_duplicates_total",
			Help: "Повторно доставленных событий, отброшенных по event_id",
		}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "answers_failed_total",
			Help: "Ошибок обработки сообщений",
		}),
	}
	reg.MustRegister(m.lag, m.saved, m.duplicates, m.failed)
	return m
}
