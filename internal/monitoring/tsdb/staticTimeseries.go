package tsdb

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nm-morais/demmon-common/body_types"
)

type ReadOnlyTimeSeries interface {
	Name() string
	Tags() map[string]string
	Tag(string) (string, bool)
	SetTag(string, string)
	All() []Observable
	Range(start time.Time, end time.Time) ([]Observable, error)
	Last() Observable
	ToDTO() body_types.TimeseriesDTO
}

type StaticTimeseries struct {
	mu              *sync.Mutex
	MeasurementName string
	TSTags          map[string]string
	Values          []Observable
}

func StaticTimeseriesFromDTO(dto body_types.TimeseriesDTO) ReadOnlyTimeSeries {
	ts := &StaticTimeseries{MeasurementName: dto.MeasurementName, TSTags: dto.TSTags, mu: &sync.Mutex{}}
	for _, pt := range dto.Values {
		ts.Values = append(ts.Values, NewObservable(pt.Fields, pt.TS))
	}
	return ts
}

func NewStaticTimeSeries(measurementName string, tags map[string]string, values ...Observable) ReadOnlyTimeSeries {
	ts := &StaticTimeseries{MeasurementName: measurementName, TSTags: tags, Values: values, mu: &sync.Mutex{}}
	return ts
}

func (t *StaticTimeseries) SetName(newName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.MeasurementName = newName
}

func (t *StaticTimeseries) Name() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.MeasurementName
}

func (t *StaticTimeseries) Tags() map[string]string {
	t.mu.Lock()
	defer t.mu.Unlock()

	tagsCopy := map[string]string{}

	for tagKey, tagVal := range t.TSTags {
		tagsCopy[tagKey] = tagVal
	}

	return tagsCopy
}

func (t *StaticTimeseries) SetTag(key, val string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.TSTags == nil {
		t.TSTags = make(map[string]string)
	}

	t.TSTags[key] = val
}

func (t *StaticTimeseries) Tag(key string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.TSTags == nil {
		return "", false
	}
	tag, ok := t.TSTags[key]
	return tag, ok
}

func (t *StaticTimeseries) All() []Observable {
	t.mu.Lock()
	defer t.mu.Unlock()

	toReturn := make([]Observable, 0)

	for i := len(t.Values) - 1; i >= 0; i-- {
		curr := t.Values[i]
		if curr == nil {
			continue
		}

		toReturn = append(toReturn, curr)
	}

	return toReturn
}

// Recent returns the last value inserted.
func (t *StaticTimeseries) Last() Observable {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := len(t.Values) - 1; i >= 0; i-- {
		curr := t.Values[i]
		if curr == nil {
			continue
		}

		return curr
	}

	return nil
}

// func (t *StaticTimeseries) UnmarshalJSON(b []byte) error {
// 	var stuff map[string]string
// 	err := json.Unmarshal(b, &stuff)

// 	if err != nil {
// 		return err
// 	}

// 	err = json.Unmarshal([]byte(stuff["tags"]), &t.TSTags)
// 	if err != nil {
// 		return err
// 	}

// 	err = json.Unmarshal([]byte(stuff["values"]), &t.Values)

// 	if err != nil {
// 		return err
// 	}

// 	t.MeasurementName = stuff["name"]

// 	return nil
// }

// Add records an observation at the current time.
func (t *StaticTimeseries) String() string {

	var sb = strings.Builder{}

	sb.WriteString(fmt.Sprintf("Name: %s", t.Name()))
	sb.WriteString(" | Tags: ")
	sb.WriteString("[")
	for tagKey, tagVal := range t.Tags() {
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

func (t *StaticTimeseries) Range(start, end time.Time) ([]Observable, error) {
	panic("not implemented")
}

func (t *StaticTimeseries) ToDTO() body_types.TimeseriesDTO {
	toReturn := body_types.TimeseriesDTO{
		MeasurementName: t.Name(),
		TSTags:          t.Tags(),
	}
	for _, pt := range t.All() {
		toReturn.Values = append(toReturn.Values, body_types.NewObservableDTO(pt.Value(), pt.TS()))
	}
	return toReturn
}

// func (t *StaticTimeseries) Count() int {
// 	panic("not implemented")
// }

// func (t *StaticTimeseries) Frequency() time.Duration {
// 	panic("not implemented")
// }

// func (t *StaticTimeseries) AddPoint(p Observable) {
// 	panic("not implemented")
// }

// func (t *StaticTimeseries) Clear() {
// 	panic("not implemented")
// }
