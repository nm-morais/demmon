package tsdb

import (
	"fmt"
	"strings"
	"time"
)

// An Observable is a kind of data that can be aggregated in a time series.
type Observable interface {
	Clear()                    // Clears the observation so it can be reused.
	CopyFrom(other Observable) // Copies the contents of a given observation to self
	TS() time.Time
	Value() map[string]interface{}
	Clone() Observable
	String() string
}

type observable struct {
	ObsTS     time.Time              `json:"timestamp"`
	ObsFields map[string]interface{} `json:"fields"`
}

func NewObservable(fields map[string]interface{}, ts time.Time) Observable {
	if len(fields) == 0 {
		panic("empty fields")
	}

	return &observable{
		ObsTS:     ts,
		ObsFields: fields,
	}
}

// Value returns the float's value.
func (f *observable) Value() map[string]interface{} {
	toReturn := make(map[string]interface{}, len(f.ObsFields))
	for k, v := range f.ObsFields {
		toReturn[k] = v
	}

	return toReturn
}

func (f *observable) Clone() Observable {
	toReturn := NewObservable(f.Value(), f.TS())
	return toReturn
}

func (f *observable) TS() time.Time {
	return f.ObsTS
}

func (f *observable) Clear() {
	for k := range f.ObsFields {
		delete(f.ObsFields, k)
	}
}

func (f *observable) CopyFrom(other Observable) {
	otherFields := other.Value()
	f.ObsFields = make(map[string]interface{}, len(otherFields))

	for k, v := range otherFields {
		f.ObsFields[k] = v
	}

	f.ObsTS = other.TS()
}

func (f *observable) String() string {
	if f == nil {
		return "<nil>"
	}

	if len(f.ObsFields) == 0 {
		return "<empty field>"
	}

	var sb strings.Builder

	sb.WriteString("[")

	for fieldKey, field := range f.ObsFields {
		sb.WriteString(fmt.Sprintf("%s:%+v, ", fieldKey, field))
	}

	sb.WriteString("]")

	return sb.String()
}
