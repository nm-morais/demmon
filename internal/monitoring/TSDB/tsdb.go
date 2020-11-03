package tsdb

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	timeseries "github.com/nm-morais/demmon-common/timeseries"
	"github.com/nm-morais/demmon/internal/monitoring/app_errors"
	"github.com/nm-morais/trie"
)

var (
	NotFoundErr      = errors.New("timeseries not found")
	AlreadyExistsErr = errors.New("timeseries already exists")
)

var createOnce sync.Once
var db *TSDB

type TSDB struct {
	t *trie.PathTrie
}

func GetDB() *TSDB {
	if db == nil {
		createOnce.Do(func() {
			db = &TSDB{
				t: trie.NewPathTrie(),
			}
		})
	}
	return db
}

func (db *TSDB) AddTimeseries(tsName string, opts ...timeseries.Option) *app_errors.AppError {
	_, err := db.GetTimeseries(tsName)
	if err == nil { // ts already exists
		return app_errors.New(AlreadyExistsErr, http.StatusConflict)
	}
	newTs, createErr := timeseries.NewTimeSeries(opts...)
	if createErr != nil {
		if errors.Is(createErr, timeseries.ErrNilGranularities) {
			return app_errors.New(createErr, http.StatusBadRequest)
		}
		return app_errors.New(createErr, http.StatusInternalServerError)
	}
	fmt.Printf("Registered metric %s\n", tsName)
	db.t.Put(tsName, newTs)
	return nil
}

func (db *TSDB) AddMetricCurrTime(metricName string, value timeseries.Value) *app_errors.AppError {
	ts, err := db.GetTimeseries(metricName)
	if err != nil {
		return err
	}
	p := &timeseries.PointValue{
		Time:  time.Now(),
		Value: value,
	}
	ts.AddPoint(p)
	return nil
}

func (db *TSDB) DeleteTimeseries(tsName string) *app_errors.AppError {
	_, err := db.GetTimeseries(tsName)
	if err != nil {
		return err
	}
	db.t.Delete(tsName)
	return nil
}

func (db *TSDB) AddMetricAtTime(name string, v timeseries.Value, t time.Time) *app_errors.AppError {
	ts, err := db.GetTimeseries(name)
	if err != nil {
		return err
	}
	p := &timeseries.PointValue{
		Time:  t,
		Value: v,
	}
	ts.AddPoint(p)
	return nil
}

func (db *TSDB) GetTimeseries(tsName string) (timeseries.TimeSeries, *app_errors.AppError) {
	res := db.t.Get(tsName)
	if res == nil {
		return nil, app_errors.New(NotFoundErr, http.StatusNotFound)
	}
	return res.(timeseries.TimeSeries), nil
}

func (db *TSDB) GetTimeseriesByPrefix(prefix string) ([]timeseries.TimeSeries, error) {
	tsCollection := make([]timeseries.TimeSeries, 0)
	err := db.t.Walk(prefix, func(k string, v interface{}) error {
		ts := v.(timeseries.TimeSeries)
		tsCollection = append(tsCollection, ts)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tsCollection, nil
}

func (db *TSDB) GetRegisteredTimeseries() ([]string, *app_errors.AppError) {
	tsCollection := make([]string, 0)
	err := db.t.Walk("", func(k string, _ interface{}) error {
		tsCollection = append(tsCollection, k)
		return nil
	})
	if err != nil {
		return nil, app_errors.New(err, http.StatusInternalServerError)
	}
	return tsCollection, nil
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
