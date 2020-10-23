package tsdb

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	timeseries "github.com/nm-morais/demmon/internal/monitoring/metrics/timeSeries"
)

const tsdb = "tsdb"

type TSDB struct {
	metricValues *sync.Map
}

func NewTSDB() *TSDB {
	return &TSDB{
		metricValues: &sync.Map{},
	}
}

func (db *TSDB) GetActiveMetrics() []string {
	metricNames := make([]string, 0)
	db.metricValues.Range(func(k, _ interface{}) bool {
		metricNames = append(metricNames, k.(string))
		return true
	})
	return metricNames
}

func (db *TSDB) AddMetricBlob(metrics []byte) error {
	metricsStr := string(metrics)
	lines := strings.Split(metricsStr, `\n`)
	for lineNr, line := range lines {
		line = strings.TrimSpace(line)
		lineSplit := strings.Split(line, ` `)
		if len(lineSplit) != 2 {
			return fmt.Errorf("Invalid metric in line %d (%s)", lineNr, line)
		}

		metricName := lineSplit[0]
		metricValStr := lineSplit[1]
		metricVal, err := strconv.ParseFloat(metricValStr, 32)
		if err != nil {
			return err
		}
		db.AddMetric(metricName, metricVal)
	}
	return nil
}

func (db *TSDB) RegisterMetric(name string, opts ...timeseries.Option) error {
	newTs, err := timeseries.NewTimeSeries(opts...)
	if err != nil {
		return err
	}
	db.metricValues.Store(name, newTs)
	return nil
}

func (db *TSDB) AddMetric(name string, value float64) error {
	var series timeseries.TimeSeries
	seriesInt, ok := db.metricValues.Load(name)
	if !ok {
		return errors.New("Metric not installed")
	}
	series = seriesInt.(timeseries.TimeSeries)
	series.Add(value)
	return nil
}

func (db *TSDB) AddMetricAtTime(name string, value float64, time time.Time) error {
	var series timeseries.TimeSeries
	seriesInt, ok := db.metricValues.Load(name)
	if !ok {
		return errors.New("Metric not installed")
	}
	series = seriesInt.(timeseries.TimeSeries)
	series.AddAt(value, time)
	return nil
}

func (db *TSDB) GetTimeseries(name string) (timeseries.TimeSeries, error) {
	seriesInt, ok := db.metricValues.Load(name)
	if !ok {
		return nil, errors.New("timeseries not found")
	}
	return seriesInt.(timeseries.TimeSeries), nil
}

// func (db *TSDB) SetMetricTimeFrame(metricName, frame string) {
// 	seriesInt, ok := db.metricValues.Load(metricName)
// 	newTs, err := timeseries.NewTimeSeries(frame)
// 	if ok {
// 		series := seriesInt.(timeseries.TimeSeries)
// 		newTs.Add(series.Last())
// 		return
// 	}
// 	db.metricValues.Store(metricName, newTs)
// }
