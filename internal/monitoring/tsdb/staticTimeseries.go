package tsdb

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type staticTimeseries struct {
	mu              *sync.Mutex
	measurementName string
	tags            map[string]string
	values          []Observable
}

func NewStaticTimeSeries(measurementName string, tags map[string]string, values []Observable) TimeSeries {
	return &staticTimeseries{measurementName: measurementName, tags: tags, values: values, mu: &sync.Mutex{}}
}

func (t *staticTimeseries) Name() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.measurementName
}

func (t *staticTimeseries) Tags() map[string]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	tagsCopy := map[string]string{}

	for tagKey, tagVal := range t.tags {

		tagsCopy[tagKey] = tagVal
	}

	return tagsCopy
}

func (t *staticTimeseries) SetTag(key, val string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.tags == nil {
		t.tags = make(map[string]string)
	}

	t.tags[key] = val
}

func (t *staticTimeseries) All() []Observable {
	t.mu.Lock()
	defer t.mu.Unlock()

	toReturn := make([]Observable, 0)

	for i := len(t.values) - 1; i >= 0; i-- {
		curr := t.values[i]
		if curr == nil {
			continue
		}

		toReturn = append(toReturn, curr)
	}

	return toReturn
}

// Recent returns the last value inserted.
func (t *staticTimeseries) Last() Observable {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := len(t.values) - 1; i >= 0; i-- {
		curr := t.values[i]
		if curr == nil {
			continue
		}

		return curr
	}

	return nil
}

// Add records an observation at the current time.
func (t *staticTimeseries) String() string {

	var sb = strings.Builder{}

	sb.WriteString(fmt.Sprintf("Name: %s", t.Name()))
	sb.WriteString(" | Tags: ")
	sb.WriteString("[")
	for tagKey, tagVal := range t.tags {
		sb.WriteString(fmt.Sprintf("%s:%s, ", tagKey, tagVal))
	}

	sb.WriteString("]")
	sb.WriteString(" | Fields: ")
	for _, field := range t.All() {
		if field != nil {
			sb.WriteString(field.String())
		}
	}

	return sb.String()
}

func (t *staticTimeseries) MarshalJSON() ([]byte, error) {
	panic("not implemented")
}

func (t *staticTimeseries) Count() int {
	panic("not implemented")
}

func (t *staticTimeseries) Frequency() time.Duration {
	panic("not implemented")
}

func (t *staticTimeseries) AddPoint(p Observable) {
	panic("not implemented")
}

func (t *staticTimeseries) Clear() {
	panic("not implemented")
}

func (t *staticTimeseries) Range(start, end time.Time) ([]Observable, error) {
	panic("not implemented")
}
