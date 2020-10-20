package importer

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/nm-morais/go-babel/pkg/errors"
)

const tsdb = "tsdb"

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

func (db *TSDB) AddMetricBlob(metrics []byte) errors.Error {
	metricsStr := string(metrics)
	lines := strings.Split(metricsStr, `\n`)
	for lineNr, line := range lines {
		line = strings.TrimSpace(line)
		lineSplit := strings.Split(line, ` `)
		if len(lineSplit) != 2 {
			return errors.NonFatalError(500, fmt.Sprintf("Invalid metric in line %d (%s)", lineNr, line), tsdb)
		}

		metricName := lineSplit[0]
		metricValStr := lineSplit[1]
		metricVal, err := strconv.ParseFloat(metricValStr, 32)
		if err != nil {
			return errors.NonFatalError(500, err.Error(), tsdb)
		}
		db.AddMetric(metricName, metricVal)
	}
	return nil
}

func (db *TSDB) AddMetric(name string, value float64) { // TODO batch
	db.mux.Lock()
	defer db.mux.Unlock()
	db.metricValues[name] = value
}
