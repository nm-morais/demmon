package tsdb

import (
	"errors"
	"sync"
	"time"
)

var DefaultGranularity = Granularity{time.Second * 5, 60} // 5 minutes total

var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrAlreadyExists  = errors.New("timeseries already exists")
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

func (db *TSDB) GetOrCreateBucket(name string) *Bucket {
	newBucket, _ := NewBucket(name, DefaultGranularity)
	bucket, _ := db.buckets.LoadOrStore(name, newBucket)
	return bucket.(*Bucket)
}

func (db *TSDB) GetOrCreateTimeseries(name string, tags map[string]string) TimeSeries {
	b := db.GetOrCreateBucket(name)
	return b.GetOrCreateTimeseries(tags)
}

func (db *TSDB) AddMetric(bucketName string, tags map[string]string, fields map[string]interface{}, timestamp time.Time) error {
	timeseries := db.GetOrCreateTimeseries(bucketName, tags)
	// fmt.Printf("Adding point at time %+v", timestamp)
	timeseries.AddPoint(&PointValue{TS: timestamp, Fields: fields})
	return nil
}

func (db *TSDB) DeleteBucket(name string, tags map[string]string) bool {
	b, ok := db.buckets.LoadAndDelete(name)
	if !ok {
		return false
	}
	b.(*Bucket).ClearBucket()
	return true
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
