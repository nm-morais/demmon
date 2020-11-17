package tsdb

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Clock specifies the needed time related functions used by the time series.
// To use a custom clock implement the interface and pass it to the time series constructor.
// The default clock uses time.Now()
type Clock interface {
	Now() time.Time
}

// defaultClock is used in case no clock is provided to the constructor.
type defaultClock struct{}

func (c *defaultClock) Now() time.Time {
	return time.Now()
}

type Bucket struct {
	name        string
	granularity Granularity
	timeseries  *sync.Map
}

func NewBucket(name string, g Granularity) (*Bucket, error) {
	err := checkGranularity(g)
	if err != nil {
		return nil, err
	}
	return &Bucket{
		name:        name,
		timeseries:  &sync.Map{},
		granularity: g,
	}, nil
}

func (b *Bucket) ClearBucket() {
	b.timeseries.Range(func(key interface{}, value interface{}) bool {
		value.(TimeSeries).Clear()
		b.timeseries.Delete(key)
		return true
	})
}

func (b *Bucket) GetTimeseries(tags map[string]string) (TimeSeries, bool) {
	ts, ok := b.timeseries.Load(convertTagsToTsKey(tags))
	return ts.(TimeSeries), ok
}

func (b *Bucket) GetTimeseriesRegex(tagsToMatch map[string]string) []TimeSeries {
	matchingTimeseries := make([]TimeSeries, 0)
	b.timeseries.Range(func(key, value interface{}) bool {
		ts := value.(TimeSeries)
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
		if allMatching {
			matchingTimeseries = append(matchingTimeseries, ts)
		}
		return true
	})
	return matchingTimeseries
}

func (b *Bucket) GetOrCreateTimeseries(tags map[string]string) TimeSeries {
	tsKey := convertTagsToTsKey(tags)
	ts, _ := b.timeseries.LoadOrStore(tsKey, NewTimeSeries(tags, b.granularity, &defaultClock{}))
	return ts.(TimeSeries)
}

func (b *Bucket) GetOrCreateTimeseriesWithClock(tags map[string]string, c Clock) TimeSeries {
	tsKey := convertTagsToTsKey(tags)
	if c == nil {
		c = &defaultClock{}
	}
	ts, _ := b.timeseries.LoadOrStore(tsKey, NewTimeSeries(tags, b.granularity, c))
	return ts.(TimeSeries)
}

func (b *Bucket) DeleteTimeseries(tags map[string]string) error {
	tsKey := convertTagsToTsKey(tags)
	tsGeneric, ok := b.timeseries.LoadAndDelete(tsKey)
	if !ok {
		return ErrBucketNotFound
	}
	ts := tsGeneric.(TimeSeries)
	ts.Clear()
	return nil
}

func convertTagsToTsKey(tags map[string]string) string {
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
