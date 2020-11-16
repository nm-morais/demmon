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
	Tags() map[string]string
	All() []*PointValue
	Range(start time.Time, end time.Time) ([]*PointValue, error)
	AddPoint(p *PointValue)
	Last() (*PointValue, error)
	Clear()
}

// Explanation
// Have several granularity buckets
// 1s, 1m, 5m, ...
// The buckets will be in circular arrays
//
// For example we could have
// 60 1s buckets to make up 1 minute
// 60 1m buckets to make up 1 hour
// ...
// This would enable us to get the last 1 minute data at 1s granularity (every second)
//
// Date ranges are [start, end[
//
// Put:
// Every time an event comes we add it to all corresponding buckets
//
// Example:
// Event time = 12:00:00
// 1s bucket = 12:00:00
// 1m bucket = 12:00:00
// 5m bucket = 12:00:00
//
// Event time = 12:00:01
// 1s bucket = 12:00:01
// 1m bucket = 12:00:00
// 5m bucket = 12:00:00
//
// Event time = 12:01:01
// 1s bucket = 12:01:01
// 1m bucket = 12:01:00
// 5m bucket = 12:00:00
//
// Fetch:
// Given a time span we try to find the buckets with the finest granularity
// to satisfy the time span and return their contents
//
// Example:
// Now = 12:05:30
// Time span = 12:05:00 - 12:05:02
// Return sum of 1s buckets 0,1
//
// Now = 12:10:00
// Time span = 12:05:00 - 12:07:00
// Return sum of 1m buckets 5,6
//
// Now = 12:10:00
// Time span = 12:00:00 - 12:10:00 (last 10 minutes)
// Return sum of 5m buckets 0,1
//
// Now = 12:10:01
// Time span = 12:05:01 - 12:10:01 (last 5 minutes)
// Return sum of 5m buckets (59/(5*60))*1, (1/(5*60))*2
//
// Now = 12:10:01
// Time span = 12:04:01 - 12:10:01 (last 6 minutes)
// Return sum of 1m buckets (59/60)*4, 5, 6, 7, 8, 9, (1/60)*10

var (
	ErrNilGranularities = errors.New("timeseries: range is nil")
	// ErrBadRange indicates that the given range is invalid. Start should always be <= End
	ErrBadRange = errors.New("timeseries: range is invalid")
	// ErrBadGranularities indicates that the provided granularities are not strictly increasing
	ErrBadGranularities = errors.New("timeseries: granularities must be strictly increasing and non empty")
	// ErrRangeNotCovered indicates that the provided range lies outside the time series
	ErrRangeNotCovered = errors.New("timeseries: range is not convered")
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

type timeseries struct {
	tags map[string]string
	*sync.Mutex
	clock   Clock
	level   level
	pending *PointValue
	latest  time.Time
}

// NewTimeSeries creates a new time series with the provided options.
// If no options are provided default values are used.
func NewTimeSeries(tags map[string]string, g Granularity, clock Clock) TimeSeries {
	if clock == nil {
		clock = &defaultClock{}
	}

	// ctx := context.TODO()
	// mts, _ := utime.NewSeriesTime(ctx, "meeting time", "1D", time.Now().UTC(), false, utime.NewSeriesTimeOptions{Size: &[]int{8}[0]})
	// df.AddSeries(mts, nil)

	return &timeseries{tags: tags, clock: clock, pending: &PointValue{TS: time.Time{}}, level: createLevel(clock, g), Mutex: &sync.Mutex{}}
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

// Increase adds amount at current time.
func (t *timeseries) AddPoint(p *PointValue) {
	if p.TS.After(t.latest) {
		// fmt.Println("1st case")
		t.latest = p.TS
	}
	if p.TS.After(t.pending.TS) {
		// fmt.Println("2nd case")
		t.advance(p.TS)
		t.pending = p
	} else if p.TS.After(t.pending.TS.Add(-t.level.granularity)) {
		t.pending = p
	} else {
		// fmt.Println("4th case")
		// if p.TS.Before(t.level.latest().Add(-1 * t.level.duration())) {
		t.level.addAtTime(p.Fields, p.TS)
		// }
	}
}

func (t *timeseries) Tags() map[string]string {
	return t.tags
}

// All returns all non-nill values in bucket with biggest precision of the timeSeries
func (t *timeseries) All() []*PointValue {
	t.advance(t.clock.Now())
	return t.level.interval(t.level.earliest(), t.level.latest(), t.latest)
}

func (t *timeseries) advance(target time.Time) {
	// we need this here because advance is called from other locations
	// than IncreaseAtTime that don't check by themselves
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

func (t *timeseries) Clear() {
	t.level.clear(time.Now())
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

// Recent returns the last value inserted
func (t *timeseries) Last() (*PointValue, error) {
	if t.clock.Now().After(t.pending.TS) {
		t.advance(t.clock.Now())
	}
	res := t.level.last()
	if res != nil {
		return &PointValue{TS: t.latest, Fields: copyMap(res)}, nil
	}
	return nil, errors.New("no last value in ts")
}

// RangeValues returns the values over the given range [start, end).
// ErrBadRange is returned if start is after end.
// ErrRangeNotCovered is returned if the range lies outside the time series.
func (t *timeseries) Range(start, end time.Time) ([]*PointValue, error) {
	if start.After(end) {
		return nil, ErrBadRange
	}
	t.advance(t.clock.Now())
	if ok, err := t.intersects(start, end); !ok {
		return nil, err
	}
	// use !start.Before so earliest() is included
	// if we use earliest().Before() we won't get start
	return t.level.interval(start, end, t.latest), nil
}

func (t *timeseries) intersects(start, end time.Time) (bool, error) {
	if start.After(t.level.latest()) {
		return false, ErrRangeNotCovered
	}
	return true, nil
}
