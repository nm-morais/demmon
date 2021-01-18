package tsdb

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon/internal/utils"
	"github.com/sirupsen/logrus"
)

type Bucket struct {
	name        string
	granularity time.Duration
	count       int
	timeseries  *sync.Map
	logger      *logrus.Entry
}

func NewBucket(name string, g time.Duration, count int, logger *logrus.Entry) *Bucket {
	return &Bucket{
		logger:      logger,
		name:        name,
		timeseries:  &sync.Map{},
		granularity: g,
		count:       count,
	}
}

func (b *Bucket) ClearBucket() {
	b.logger.Info("Clearing bucket")
	b.timeseries.Range(
		func(key interface{}, value interface{}) bool {
			ts := value.(TimeSeries)
			b.logger.Infof("Clearing ts %+v", ts.Tags())
			ts.Clear()
			b.timeseries.Delete(key)
			return true
		},
	)
}

func (b *Bucket) GetTimeseries(tags map[string]string) (TimeSeries, bool) {
	ts, ok := b.timeseries.Load(convertTagsToTSKey(tags))
	if !ok {
		return nil, ok
	}

	return ts.(TimeSeries), ok
}

func (b *Bucket) GetAllTimeseries() []ReadOnlyTimeSeries {
	toReturn := make([]ReadOnlyTimeSeries, 0)

	b.logger.Infof("Getting all timeseries for bucket %s", b.name)
	b.timeseries.Range(
		func(key, value interface{}) bool {
			ts := value.(TimeSeries)
			allPts := ts.All()
			if len(allPts) == 0 {
				b.logger.Errorf("Timeseries with tags %+v has no points", ts.Tags())
				return true
			}
			toReturn = append(toReturn, NewStaticTimeSeries(ts.Name(), ts.Tags(), allPts...))
			return true
		},
	)
	return toReturn
}

func (b *Bucket) GetAllTimeseriesLast() []ReadOnlyTimeSeries {
	toReturn := make([]ReadOnlyTimeSeries, 0)

	b.timeseries.Range(
		func(key, value interface{}) bool {
			ts := value.(TimeSeries)
			lastPt := ts.Last()
			if lastPt == nil {
				return true
			}
			toReturn = append(toReturn, NewStaticTimeSeries(ts.Name(), ts.Tags(), lastPt))
			return true
		},
	)

	return toReturn
}

func (b *Bucket) GetAllTimeseriesRange(start, end time.Time) []ReadOnlyTimeSeries {
	toReturn := make([]ReadOnlyTimeSeries, 0)

	b.timeseries.Range(
		func(key, value interface{}) bool {
			ts := value.(TimeSeries)
			points, err := ts.Range(start, end)
			if err != nil {
				panic(err)
			}
			toReturn = append(toReturn, NewStaticTimeSeries(ts.Name(), ts.Tags(), points...))
			return true
		},
	)

	return toReturn
}

func filterMatchesTs(tagsToMatch map[string]string, ts TimeSeries) bool {
	tsTags := ts.Tags()
	allMatching := true
	for tagKey, tagVal := range tagsToMatch {
		timeseriesTag, hasKey := tsTags[tagKey]
		if !hasKey {
			break
		}
		matched, err := regexp.MatchString(tagVal, timeseriesTag)
		if err != nil {
			break
		}
		if !matched {
			allMatching = false
			break
		}
	}
	return allMatching
}

func (b *Bucket) getTimeseriesRegex(tagsToMatch map[string]string) []TimeSeries {
	matchingTimeseries := make([]TimeSeries, 0)

	b.timeseries.Range(
		func(key, value interface{}) bool {
			ts := value.(TimeSeries)
			if filterMatchesTs(tagsToMatch, ts) {
				matchingTimeseries = append(matchingTimeseries, ts)
			}
			return true
		},
	)

	return matchingTimeseries
}

func (b *Bucket) GetTimeseriesRegex(tagsToMatch map[string]string) []ReadOnlyTimeSeries {
	matchingTimeseries := b.getTimeseriesRegex(tagsToMatch)

	toReturn := []ReadOnlyTimeSeries{}
	for _, ts := range matchingTimeseries {
		toReturn = append(toReturn, NewStaticTimeSeries(ts.Name(), ts.Tags(), ts.All()...))
	}

	return toReturn
}

func (b *Bucket) GetTimeseriesRegexRange(tagsToMatch map[string]string, start, end time.Time) []ReadOnlyTimeSeries {
	matchingTimeseries := b.getTimeseriesRegex(tagsToMatch)
	toReturn := []ReadOnlyTimeSeries{}
	for _, ts := range matchingTimeseries {
		points, err := ts.Range(start, end)
		if err != nil {
			panic(err)
		}

		toReturn = append(toReturn, NewStaticTimeSeries(ts.Name(), ts.Tags(), points...))
	}

	return toReturn
}

func (b *Bucket) GetTimeseriesRegexLastVal(tagsToMatch map[string]string) []ReadOnlyTimeSeries {
	matchingTimeseries := b.getTimeseriesRegex(tagsToMatch)
	toReturn := []ReadOnlyTimeSeries{}
	for _, ts := range matchingTimeseries {
		last := ts.Last()
		// b.logger.Infof("Last point: %+v", last)
		if last != nil {
			toReturn = append(
				toReturn,
				NewStaticTimeSeries(ts.Name(), ts.Tags(), last),
			)
		}
	}

	return toReturn
}

func (b *Bucket) RegisterObserver(o utils.Observer, watchList []body_types.TimeseriesFilter) {
	b.timeseries.Range(func(key, value interface{}) bool {

		return true
	})
}

func (b *Bucket) DropAll() {
	b.timeseries.Range(
		func(key, value interface{}) bool {
			_ = b.DeleteTimeseries(value.(TimeSeries).Tags())
			return true
		},
	)
}

func (b *Bucket) DropTimeseriesRegex(tagsToMatch map[string]string) {
	matchingTimeseries := b.getTimeseriesRegex(tagsToMatch)
	for _, ts := range matchingTimeseries {
		_ = b.DeleteTimeseries(ts.Tags())
	}
}

func (b *Bucket) GetOrCreateTimeseries(tags map[string]string) TimeSeries {
	tsKey := convertTagsToTSKey(tags)
	ts, loaded := b.timeseries.LoadOrStore(tsKey, NewTimeSeries(b.name, tags, b.granularity, b.count, b.createLoggerForTimeseries(tags)))
	if !loaded {
		// b.notifyAll(ts.(TimeSeries))
	}
	return ts.(TimeSeries)
}

func (b *Bucket) createLoggerForTimeseries(tags map[string]string) *logrus.Entry {
	return b.logger.WithField("tags", tags)
}

func (b *Bucket) GetOrCreateTimeseriesWithClock(tags map[string]string, c Clock) TimeSeries {
	tsKey := convertTagsToTSKey(tags)
	b.logger.Infof("Creating timeseries with: name: %s, tags: %s, count:%d\n", b.name, tags, b.count)
	newTS := NewTimeSeriesWithClock(b.name, tags, b.granularity, b.count, c, b.createLoggerForTimeseries(tags))
	ts, loaded := b.timeseries.LoadOrStore(tsKey, newTS)
	if !loaded {
		b.logger.Infof("Created timeseries: %s\n", ts)
	}
	return ts.(TimeSeries)
}

func (b *Bucket) DeleteTimeseries(tags map[string]string) error {
	tsKey := convertTagsToTSKey(tags)
	tsGeneric, ok := b.timeseries.LoadAndDelete(tsKey)

	if !ok {
		return ErrBucketNotFound
	}

	ts := tsGeneric.(TimeSeries)
	ts.Clear()

	return nil
}

func convertTagsToTSKey(tags map[string]string) string {
	if tags == nil {
		return ""
	}

	tagKeys := sort.StringSlice{}
	for tagKey := range tags {
		tagKeys = append(tagKeys, tagKey)
	}

	sort.Sort(tagKeys)

	resultingKey := ""

	for _, k := range tagKeys {
		resultingKey += fmt.Sprintf("%s_%s", k, tags[k])
	}

	return resultingKey
}
