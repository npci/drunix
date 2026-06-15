/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package statesqldb

import (
	"sync"

	"github.com/npci/drunix/common/metrics"
)

var (
	DBCallRedis = metrics.CounterOpts{
		Namespace:  "DBcallRedis",
		Name:       "DBcall",
		LabelNames: []string{"method_name", "call_type", "status"},
		Help:       "The number of fail DB calls to KeyDB",
	}

	DBCallSql = metrics.CounterOpts{
		Namespace:  "DBcallSql",
		Name:       "DBcall",
		LabelNames: []string{"method_name", "call_type", "status"},
		Help:       "The number of fail DB calls to SQL",
	}

	DBCallTimeRedisOpts = metrics.HistogramOpts{
		Namespace:    "KeyDB",
		Subsystem:    "",
		Name:         "KeyDBDBCallTime",
		Help:         "Time taken in seconds for a KeyDB call",
		LabelNames:   []string{"method_name", "call_type", "status"},
		StatsdFormat: "%{#fqname}.%{channel}",
		Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	}

	DBCallTimeSQLOpts = metrics.HistogramOpts{
		Namespace:    "SQL",
		Subsystem:    "",
		Name:         "SQLDBCallTime",
		Help:         "Time taken in seconds for a SQL DB call",
		LabelNames:   []string{"method_name", "call_type", "status"},
		StatsdFormat: "%{#fqname}.%{channel}",
		Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	}

	registerMetricsOnce sync.Once
)

type Metrics struct {
	DBCallRedis     metrics.Counter
	DBCallSql       metrics.Counter
	DBCallTimeRedis metrics.Histogram
	DBCallTimeSQL   metrics.Histogram
}

func NewMetrics(p metrics.Provider) *Metrics {
	return &Metrics{
		DBCallRedis:     p.NewCounter(DBCallRedis),
		DBCallSql:       p.NewCounter(DBCallSql),
		DBCallTimeRedis: p.NewHistogram(DBCallTimeRedisOpts),
		DBCallTimeSQL:   p.NewHistogram(DBCallTimeSQLOpts),
	}
}
