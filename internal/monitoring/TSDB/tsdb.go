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

func (db *TSDB) AddTimeseries(service, name, origin string, opts ...timeseries.Option) error {
	_, err := db.GetTimeseries(service, name, origin)
	if err == nil { // ts already exists
		return ErrAlreadyExists
	}
	newTs, createErr := timeseries.NewTimeSeries(service, name, origin, opts...)
	if createErr != nil {
		if errors.Is(createErr, timeseries.ErrNilGranularities) {
			return createErr
		}
		return createErr
	}
	metricKey := fmt.Sprintf("%s/%s/%s", service, name, origin)
	fmt.Printf("Registered metric %s\n", metricKey)
	db.t.Put(metricKey, newTs)
	return nil
}

func (db *TSDB) AddMetricCurrTime(service, name, origin string, value timeseries.Value) error {
	ts, err := db.GetTimeseries(service, name, origin)
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

func (db *TSDB) DeleteTimeseries(service, name, origin string) error {
	_, err := db.GetTimeseries(service, name, origin)
	if err != nil {
		return err
	}
	metricKey := fmt.Sprintf("%s/%s/%s", service, name, origin)
	db.t.Delete(metricKey)
	return nil
}

func (db *TSDB) AddMetricAtTime(service, name, origin string, v timeseries.Value, t time.Time) error {
	ts, err := db.GetTimeseries(service, name, origin)
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

func (db *TSDB) GetTimeseries(service, name, origin string) (timeseries.TimeSeries, error) {
	metricKey := fmt.Sprintf("%s/%s/%s", service, name, origin)
	res := db.t.Get(metricKey)
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
