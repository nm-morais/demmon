package tsdb

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"
)

type Bucket struct {
	name        string
	granularity time.Duration
	count       int
	timeseries  *sync.Map
}

func NewBucket(name string, g time.Duration, count int) *Bucket {
	return &Bucket{
		name:        name,
		timeseries:  &sync.Map{},
		granularity: g,
		count:       count,
	}
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
	if !ok {
		return nil, ok
	}
	return ts.(TimeSeries), ok
}

func (b *Bucket) GetAllTimeseries() []TimeSeries {
	toReturn := make([]TimeSeries, 0)
	b.timeseries.Range(func(key, value interface{}) bool {
		ts := value.(TimeSeries)
		toReturn = append(toReturn, NewStaticTimeSeries(ts.Name(), ts.Tags(), ts.All()))
		// toReturn = append(toReturn, ts)
		return true
	})
	return toReturn
}

func (b *Bucket) GetAllTimeseriesLast() []TimeSeries {
	toReturn := make([]TimeSeries, 0)
	b.timeseries.Range(func(key, value interface{}) bool {
		ts := value.(TimeSeries)
		lastPt := ts.Last()
		if lastPt != nil {
			toReturn = append(toReturn, NewStaticTimeSeries(ts.Name(), ts.Tags(), []Observable{}))
		}
		return true
	})
	return toReturn
}

func (b *Bucket) getTimeseriesRegex(tagsToMatch map[string]string) []TimeSeries {
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
			matchingTimeseries = append(matchingTimeseries, NewStaticTimeSeries(ts.Name(), tsTags, ts.All()))
		}
		return true
	})
	return matchingTimeseries
}

func (b *Bucket) GetTimeseriesRegex(tagsToMatch map[string]string) []TimeSeries {
	matchingTimeseries := b.getTimeseriesRegex(tagsToMatch)
	for _, ts := range matchingTimeseries {
		matchingTimeseries = append(matchingTimeseries, NewStaticTimeSeries(ts.Name(), ts.Tags(), ts.All()))
	}
	return matchingTimeseries
}

func (b *Bucket) GetTimeseriesRegexRange(tagsToMatch map[string]string, start, end time.Time) []TimeSeries {
	matchingTimeseries := b.getTimeseriesRegex(tagsToMatch)
	for _, ts := range matchingTimeseries {
		points, err := ts.Range(start, end)
		if err != nil {
			panic(err)
		}
		matchingTimeseries = append(matchingTimeseries, NewStaticTimeSeries(ts.Name(), ts.Tags(), points))
	}
	return matchingTimeseries
}

func (b *Bucket) GetTimeseriesRegexLastVal(tagsToMatch map[string]string) []TimeSeries {
	matchingTimeseries := b.getTimeseriesRegex(tagsToMatch)
	for _, ts := range matchingTimeseries {
		last := ts.Last()
		if last != nil {
			matchingTimeseries = append(matchingTimeseries, NewStaticTimeSeries(ts.Name(), ts.Tags(), []Observable{last}))
		}
	}
	return matchingTimeseries
}

func (b *Bucket) DropAll() {
	b.timeseries.Range(func(key, value interface{}) bool {
		_ = b.DeleteTimeseries(value.(TimeSeries).Tags())
		return true
	})
}

func (b *Bucket) DropTimeseriesRegex(tagsToMatch map[string]string) {
	matchingTimeseries := b.getTimeseriesRegex(tagsToMatch)
	for _, ts := range matchingTimeseries {
		_ = b.DeleteTimeseries(ts.Tags())
	}
}

func (b *Bucket) GetOrCreateTimeseries(tags map[string]string) TimeSeries {
	tsKey := convertTagsToTsKey(tags)
	ts, _ := b.timeseries.LoadOrStore(tsKey, NewTimeSeries(b.name, tags, b.granularity, b.count))
	return ts.(TimeSeries)
}

func (b *Bucket) GetOrCreateTimeseriesWithClock(tags map[string]string, c Clock) TimeSeries {

	tsKey := convertTagsToTsKey(tags)
	fmt.Printf("Creating timeseries with: name: %s, tags: %s, count:%d\n", b.name, tags, b.count)

	newTs := NewTimeSeriesWithClock(b.name, tags, b.granularity, b.count, c)
	fmt.Printf("Creating timeseries:%+v\n", newTs)

	ts, _ := b.timeseries.LoadOrStore(tsKey, newTs)
	fmt.Printf("Created timeseries: %s\n", ts)

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
