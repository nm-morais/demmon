package tsdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrAlreadyExists  = errors.New("bucket already exists")
)

var createOnce sync.Once
var db *TSDB

type TSDB struct {
	logger  *logrus.Logger
	buckets *sync.Map
}

type Conf struct {
	LogFile          string
	LogFolder        string
	Silent           bool
	SetupLogToFile   bool
	CleanupFrequency time.Duration
}

func GetDB(conf *Conf) *TSDB {
	if db == nil {
		createOnce.Do(func() {
			logger := &logrus.Logger{
				Level: logrus.InfoLevel,
			}
			if conf.SetupLogToFile {
				setupLogger(logger, conf.LogFolder, conf.LogFile, conf.Silent)
			}

			db = &TSDB{
				logger:  logger,
				buckets: &sync.Map{},
			}
			go db.cleanupTimeseries(conf.CleanupFrequency)
		})
	}

	return db
}

func (db *TSDB) cleanupTimeseries(tickerDuration time.Duration) {
	ticker := time.NewTicker(tickerDuration)
	for range ticker.C {
		db.buckets.Range(func(key, value interface{}) bool {
			bucket := value.(*Bucket)
			bucket.cleanup()
			return true
		})
	}
}

func (db *TSDB) GetBucket(name string) (*Bucket, bool) {
	bucket, ok := db.buckets.Load(name)
	if !ok {
		return nil, false
	}

	return bucket.(*Bucket), ok
}

func (db *TSDB) AddAll(toAdd []ReadOnlyTimeSeries) error {
	for _, ts := range toAdd {
		allPts := ts.All()
		if len(allPts) == 0 {
			db.logger.Error("err: Cannot add timeseries to DB because it is empty")
			continue
		}
		for _, pt := range allPts {
			err := db.AddMetric(ts.Name(), ts.Tags(), pt.Value(), pt.TS())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *TSDB) GetOrCreateTimeseries(
	name string,
	tags map[string]string,
	frequency time.Duration,
	count int,
) (TimeSeries, error) {
	b, ok := db.GetBucket(name)
	if !ok {
		return nil, ErrBucketNotFound
	}

	return b.GetOrCreateTimeseries(tags), nil
}

func (db *TSDB) GetTimeseries(name string, tags map[string]string) (TimeSeries, bool) {
	b, ok := db.GetBucket(name)
	if !ok {
		return nil, false
	}

	return b.GetTimeseries(tags)
}

func (db *TSDB) GetOrCreateTimeseriesWithClock(name string, tags map[string]string, clock Clock) (TimeSeries, error) {
	b, ok := db.GetBucket(name)
	if !ok {
		return nil, ErrBucketNotFound
	}

	return b.GetOrCreateTimeseriesWithClock(tags, clock), nil
}

func (db *TSDB) AddMetric(
	bucketName string,
	tags map[string]string,
	fields map[string]interface{},
	timestamp time.Time,
) error {
	b, hasBucket := db.GetBucket(bucketName)
	if !hasBucket {
		return ErrBucketNotFound
	}

	timeseries := b.GetOrCreateTimeseries(tags)

	pv := NewObservable(fields, timestamp)
	timeseries.AddPoint(pv)
	return nil
}

func (db *TSDB) DeleteBucket(name string, tags map[string]string) bool {
	db.logger.Infof("Deleting bucket %s", name)
	b, ok := db.buckets.LoadAndDelete(name)
	if !ok {
		db.logger.Infof("bucket %s not present", name)
		return false
	}
	b.(*Bucket).ClearBucket()
	db.logger.Infof("deleted bucket %s", name)
	return true
}

func (db *TSDB) CreateBucket(name string, frequency time.Duration, count int) (*Bucket, error) {

	db.logger.Infof("Creating new bucket with name: %s", name)
	newBucket := NewBucket(name, frequency, count, db.createEntryForBucket(name))
	bucketGeneric, loaded := db.buckets.LoadOrStore(name, newBucket)
	bucket := bucketGeneric.(*Bucket)
	if loaded {
		db.logger.Infof("bucket %s already exists", name)
		if bucket.count == count && bucket.granularity == frequency {
			return bucket, nil
		}
		return nil, ErrAlreadyExists
	}

	db.logger.Infof("Created new bucket with name: %s", name)
	return bucket, nil
}

func (db *TSDB) createEntryForBucket(name string) *logrus.Entry {
	return db.logger.WithField("bucket_name", name)
}

func (db *TSDB) GetRegisteredBuckets() []string {
	toReturn := make([]string, 0)

	db.buckets.Range(
		func(key, value interface{}) bool {
			toReturn = append(toReturn, key.(string))
			return true
		},
	)

	return toReturn
}

type formatter struct {
	owner string
	lf    logrus.Formatter
}

func (f *formatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Message = fmt.Sprintf("[%s] %s", f.owner, e.Message)
	return f.lf.Format(e)
}

func setupLogger(logger *logrus.Logger, logFolder, logFile string, silent bool) {
	logger.SetFormatter(
		&formatter{
			owner: "tsdb",
			lf: &logrus.TextFormatter{
				DisableColors:   true,
				ForceColors:     false,
				FullTimestamp:   true,
				TimestampFormat: time.StampMilli,
			},
		},
	)

	if logFolder == "" {
		logger.Panicf("Invalid logFolder '%s'", logFolder)
	}
	if logFile == "" {
		logger.Panicf("Invalid logFile '%s'", logFile)
	}

	filePath := fmt.Sprintf("%s/%s", logFolder, logFile)
	err := os.MkdirAll(logFolder, 0777)
	if err != nil {
		logger.Panic(err)
	}

	file, err := os.Create(filePath)
	if os.IsExist(err) {
		var err = os.Remove(filePath)
		if err != nil {
			logger.Panic(err)
		}
		file, err = os.Create(filePath)
		if err != nil {
			logger.Panic(err)
		}
	}
	var out io.Writer = file
	if !silent {
		out = io.MultiWriter(os.Stdout, file)
	}
	logger.SetOutput(out)
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
