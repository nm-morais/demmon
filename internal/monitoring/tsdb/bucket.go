package tsdb

import (
	"fmt"
	"sort"
	"sync"
)

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

func (b *Bucket) GetOrCreateTimeseries(tags map[string]string) TimeSeries {
	tsKey := convertTagsToTsKey(tags)
	ts, _ := b.timeseries.LoadOrStore(tsKey, NewTimeSeries(tags, b.granularity, nil))
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
