package importer

import (
	"strconv"
	"strings"
	"sync"
)

type TSDB struct {
	mux          *sync.RWMutex
	metricValues map[string]float64
}

func NewTSDB() *TSDB {
	return &TSDB{
		mux:          &sync.RWMutex{},
		metricValues: make(map[string]float64),
	}
}

func (db *TSDB) GetActiveMetrics(metricName string) []string {
	db.mux.RLock()
	defer db.mux.RUnlock()
	metricNames := make([]string, 0, len(db.metricValues))
	for metricName := range db.metricValues {
		metricNames = append(metricNames, metricName)
	}
	return metricNames
}

func (db *TSDB) ParseMetrics(metrics []byte) {
	metricsStr := string(metrics)
	lines := strings.Split(metricsStr, `\n`)
	for _, line := range lines {
		lineSplit := strings.Split(line, ` `)
		var metricName, metricValStr string

		if len(lineSplit) <= 1 {
			continue
		}

		metricName = lineSplit[0]
		metricValStr = lineSplit[1]
		metricVal, err := strconv.ParseFloat(metricValStr, 32)
		if err != nil {
			continue
		}
		db.AddMetric(metricName, metricVal)
	}
}

func (db *TSDB) AddMetric(name string, value float64) { // TODO batch
	db.mux.Lock()
	defer db.mux.Unlock()
	db.metricValues[name] = value

}
