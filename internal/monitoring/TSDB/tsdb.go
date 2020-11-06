package tsdb

import (
	"errors"
	"fmt"
	"sync"
	"time"

	timeseries "github.com/nm-morais/demmon-common/timeseries"
	"github.com/nm-morais/trie"
)

var (
	ErrNotFound      = errors.New("timeseries not found")
	ErrAlreadyExists = errors.New("timeseries already exists")
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

func (db *TSDB) AddTimeseries(tsName string, opts ...timeseries.Option) error {
	_, err := db.GetTimeseries(tsName)
	if err == nil { // ts already exists
		return ErrAlreadyExists
	}
	newTs, createErr := timeseries.NewTimeSeries(opts...)
	if createErr != nil {
		if errors.Is(createErr, timeseries.ErrNilGranularities) {
			return createErr
		}
		return createErr
	}
	fmt.Printf("Registered metric %s\n", tsName)
	db.t.Put(tsName, newTs)
	return nil
}

func (db *TSDB) AddMetricCurrTime(metricName string, value timeseries.Value) error {
	ts, err := db.GetTimeseries(metricName)
	if err != nil {
		return err
	}
	p := &timeseries.PointValue{
		TS:    time.Now(),
		Value: value,
	}
	ts.AddPoint(p)
	return nil
}

func (db *TSDB) DeleteTimeseries(tsName string) error {
	_, err := db.GetTimeseries(tsName)
	if err != nil {
		return err
	}
	db.t.Delete(tsName)
	return nil
}

func (db *TSDB) AddMetricAtTime(name string, v timeseries.Value, t time.Time) error {
	ts, err := db.GetTimeseries(name)
	if err != nil {
		return err
	}
	p := &timeseries.PointValue{
		TS:    t,
		Value: v,
	}
	ts.AddPoint(p)
	return nil
}

func (db *TSDB) GetTimeseries(tsName string) (timeseries.TimeSeries, error) {
	res := db.t.Get(tsName)
	if res == nil {
		return nil, ErrNotFound
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

func (db *TSDB) GetRegisteredTimeseries() ([]string, error) {
	tsCollection := make([]string, 0)
	err := db.t.Walk("", func(k string, _ interface{}) error {
		tsCollection = append(tsCollection, k)
		return nil
	})
	if err != nil {
		return nil, err
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
