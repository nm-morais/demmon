package tsdb

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var DefaultGranularity = Granularity{time.Second * 5, 60} // 5 minutes total

var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrAlreadyExists  = errors.New("Bucket already exists")
)

var createOnce sync.Once
var db *TSDB

type TSDB struct {
	buckets *sync.Map
}

func GetDB() *TSDB {
	if db == nil {
		createOnce.Do(func() {
			db = &TSDB{
				buckets: &sync.Map{},
			}
		})
	}
	return db
}

func (db *TSDB) GetOrCreateBucket(name string, granularity Granularity) (*Bucket, bool) {
	newBucket, _ := NewBucket(name, granularity)
	bucket, loaded := db.buckets.LoadOrStore(name, newBucket)
	return bucket.(*Bucket), loaded
}

func (db *TSDB) GetBucket(name string) (*Bucket, bool) {
	bucket, ok := db.buckets.Load(name)
	if !ok {
		return nil, false
	}
	return bucket.(*Bucket), ok
}

func (db *TSDB) GetOrCreateTimeseries(name string, tags map[string]string) TimeSeries {
	b, _ := db.GetOrCreateBucket(name, DefaultGranularity)
	return b.GetOrCreateTimeseries(tags)
}

func (db *TSDB) GetTimeseries(name string, tags map[string]string) (TimeSeries, bool) {
	b, ok := db.GetBucket(name)
	if !ok {
		return nil, false
	}
	return b.GetTimeseries(tags)
}

func (db *TSDB) GetOrCreateTimeseriesWithClockAndGranularity(name string, tags map[string]string, clock Clock, g Granularity) TimeSeries {
	b, _ := db.GetOrCreateBucket(name, g)
	return b.GetOrCreateTimeseriesWithClock(tags, clock)
}

func (db *TSDB) AddMetric(bucketName string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) {
	timeseries := db.GetOrCreateTimeseries(bucketName, tags)
	fmt.Printf("TS: %s, Tags: %+v adding point %+v at time %+v \n", bucketName, tags, fields, timestamp)
	timeseries.AddPoint(PointValue{TS: timestamp, Fields: fields})
}

func (db *TSDB) DeleteBucket(name string, tags map[string]string) bool {
	b, ok := db.buckets.LoadAndDelete(name)
	if !ok {
		return false
	}
	b.(*Bucket).ClearBucket()
	return true
}

func (db *TSDB) CreateBucket(name string, granularity Granularity) (*Bucket, error) {
	newBucket, err := NewBucket(name, granularity)
	if err != nil {
		return nil, err
	}
	toReturn, loaded := db.buckets.LoadOrStore(name, newBucket)
	if loaded {
		return nil, errors.New("Bucket already exists")
	}
	return toReturn.(*Bucket), nil
}

func (db *TSDB) GetRegisteredBuckets() []string {
	toReturn := make([]string, 0)
	db.buckets.Range(func(key, value interface{}) bool {
		toReturn = append(toReturn, key.(string))
		return true
	})
	return toReturn
}

// func (db *TSDB) AddMetricAtTime(service, name, origin string, v timeseries.Value, t time.Time) error {
// 	ts, err := db.GetTimeseries(service, name, origin)
// 	if err != nil {
// 		return err
// 	}
// 	p := &timeseries.PointValue{
// 		TS:    t,
// 		Value: v,
// 	}
// 	ts.AddPoint(p)
// 	return nil
// }
