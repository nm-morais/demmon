package tsdb

import (
	"errors"
	"sync"
	"time"
)

type PointValue struct {
	TS     time.Time
	Fields map[string]interface{}
}

type Granularity struct {
	Granularity time.Duration
	Count       int
}

type TimeSeries interface {
	Name() string
	Tags() map[string]string
	All() []PointValue
	Range(start time.Time, end time.Time) ([]PointValue, error)
	AddPoint(p PointValue)
	Last() *PointValue
	Granularity() Granularity
	Clear()
}

var (
	ErrNilGranularities = errors.New("timeseries: range is nil")
	// ErrBadRange indicates that the given range is invalid. Start should always be <= End
	ErrBadRange = errors.New("timeseries: range is invalid")
	// ErrBadGranularities indicates that the provided granularities are not strictly increasing
	ErrBadGranularities = errors.New("timeseries: granularities must be strictly increasing and non empty")
	// ErrRangeNotCovered indicates that the provided range lies outside the time series
	ErrRangeNotCovered = errors.New("timeseries: range is not convered")
)

type timeseries struct {
	measurementName string
	g               Granularity
	tags            map[string]string
	clock           Clock
	level           level
	pending         *PointValue
	latest          time.Time
	mu              *sync.RWMutex
}

func NewTimeSeries(measurementName string, tags map[string]string, g Granularity, clock Clock) TimeSeries {
	if clock == nil {
		clock = &defaultClock{}
	}
	return &timeseries{measurementName: measurementName, tags: tags, clock: clock, pending: &PointValue{TS: time.Time{}}, level: createLevel(clock, g), g: g, mu: &sync.RWMutex{}}
}

func checkGranularity(granularity Granularity) error {
	if granularity.Count == 0 {
		return ErrBadGranularities
	}
	if granularity.Granularity == 0 {
		return ErrBadGranularities
	}
	return nil
}

func createLevel(clock Clock, g Granularity) level {
	return newLevel(clock, g.Granularity, g.Count)
}

func (t *timeseries) AddPoint(p PointValue) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p.TS.After(t.latest) {
		t.latest = p.TS
	}
	if p.TS.After(t.pending.TS) {
		t.advance(p.TS)
		t.pending.TS = p.TS
		t.pending.Fields = p.Fields
	} else if p.TS.After(t.pending.TS.Add(-t.level.granularity)) {
		t.pending.TS = p.TS
		t.pending.Fields = p.Fields
	} else {
		t.level.addAtTime(p.Fields, p.TS)
	}
}

func (t *timeseries) Name() string {
	return t.measurementName
}

func (t *timeseries) Tags() map[string]string {
	auxMap := make(map[string]string)
	for tName, tVal := range t.tags {
		auxMap[tName] = tVal
	}
	return auxMap
}

func (t *timeseries) All() []PointValue {
	t.mu.Lock()
	t.advance(t.clock.Now())
	t.mu.Unlock()
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.level.interval(t.level.earliest(), t.level.latest(), t.latest)
}

func (t *timeseries) Clear() {
	t.level.clear(time.Now())
}

// Recent returns the last value inserted
func (t *timeseries) Last() *PointValue {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.clock.Now().After(t.pending.TS) {
		t.advance(t.clock.Now())
	}
	res := t.level.last()
	if res != nil {
		return &PointValue{TS: t.latest, Fields: res}
	}
	return nil
}

func (t *timeseries) Range(start, end time.Time) ([]PointValue, error) {
	if start.After(end) {
		return nil, ErrBadRange
	}
	t.mu.Lock()
	t.advance(t.clock.Now())
	t.mu.Unlock()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if ok, err := t.intersects(start, end); !ok {
		return nil, err
	}
	// use !start.Before so earliest() is included
	// if we use earliest().Before() we won't get start
	return t.level.interval(start, end, t.latest), nil
}

func (t *timeseries) Granularity() Granularity {
	return t.g
}

func (t *timeseries) handlePending() {
	t.setAtTime(t.pending.Fields, t.pending.TS)
	t.pending.Fields = nil
	t.pending.TS = t.level.latest()
}

func (t *timeseries) setAtTime(values map[string]interface{}, time time.Time) {
	if !time.Before(t.level.latest().Add(-1 * t.level.duration())) {
		t.level.addAtTime(values, time)
	}
}

func (t *timeseries) advance(target time.Time) {
	if !target.After(t.pending.TS) {
		return
	}
	t.advanceLevels(target)
	t.handlePending()
}

func (t *timeseries) advanceLevels(target time.Time) {
	if !target.Before(t.level.latest().Add(t.level.duration())) {
		t.level.clear(target)
	}
	t.level.advance(target)
}

func (t *timeseries) intersects(start, end time.Time) (bool, error) {
	if start.After(t.level.latest()) {
		return false, ErrRangeNotCovered
	}
	return true, nil
}
