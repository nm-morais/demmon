package tsdb

import (
	"time"
)

type staticTimeseries struct {
	measurementName string
	tags            map[string]string
	values          []PointValue
}

func NewStaticTimeSeries(measurementName string, tags map[string]string, values []PointValue) TimeSeries {
	return &staticTimeseries{measurementName: measurementName, tags: tags, values: values}
}

func (t *staticTimeseries) Name() string {
	return t.measurementName
}

func (t *staticTimeseries) Tags() map[string]string {
	return t.tags
}

func (t *staticTimeseries) All() []PointValue {
	return t.values
}

// Recent returns the last value inserted
func (t *staticTimeseries) Last() *PointValue {
	if len(t.values) == 0 {
		return nil
	}
	c := t.values[len(t.values)-1]
	return &c
}

func (t *staticTimeseries) AddPoint(p PointValue) {
	panic("not implemented")
}

func (t *staticTimeseries) Clear() {
	panic("not implemented")
}

func (t *staticTimeseries) Range(start, end time.Time) ([]PointValue, error) {
	panic("not implemented")
}

func (t *staticTimeseries) Granularity() Granularity {
	panic("not implemented")
}
