package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Ivanvnew75/libs/common"
)

// Summary — то же, что отдаёт analytics. Описано здесь заново,
// а не импортировано: клиент зависит от КОНТРАКТА соседа (его JSON),
// а не от его внутренних типов. Импорт типа из чужого сервиса связал бы
// их сборки — и обновление analytics ломало бы компиляцию recommender.
type Summary struct {
	Total    uint64  `json:"total"`
	AvgScore float64 `json:"avg_score"`
	Last7    float64 `json:"avg_last_7d"`
	Prev7    float64 `json:"avg_prev_7d"`
	Trend    string  `json:"trend"`
}

type analyticsClient struct {
	base string
	c    *common.Client
}

func newAnalyticsClient(baseURL string, timeout time.Duration, retries int) *analyticsClient {
	return &analyticsClient{
		base: strings.TrimRight(baseURL, "/"),
		c:    common.NewClient(timeout, retries),
	}
}

func (a *analyticsClient) Summary(ctx context.Context, userID int64) (Summary, error) {
	var out Summary
	url := fmt.Sprintf("%s/analytics/summary?user_id=%d", a.base, userID)
	return out, a.c.DoJSON(ctx, http.MethodGet, url, nil, &out)
}
