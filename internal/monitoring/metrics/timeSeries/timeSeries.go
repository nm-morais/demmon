package timeseries

import (
	"errors"
	"sync"
	"time"
)

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

// defaultGranularities are used in case no granularities are provided to the constructor.
var defaultGranularities = []Granularity{
	{time.Second, 60},
	{time.Minute, 60},
	{time.Hour, 24},
}

type PointValue struct {
	Time  time.Time
	Value float64
}

// Metric is a single meter (counter, gauge or histogram, optionally - with history)
type TimeSeries interface {
	All() []PointValue
	AddAt(n float64, emit time.Time)
	Add(n float64)
	Last() (*PointValue, error)
	// MergeWith(series Timeseries) error
	Range(start time.Time, end time.Time) ([]PointValue, error)
	sync.Locker
}

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

// Granularity describes the granularity for one level of the time series.
// Count cannot be 0.
type Granularity struct {
	Granularity time.Duration
	Count       int
}

type options struct {
	clock         Clock
	granularities []Granularity
}

// Option configures the time series.
type Option func(*options)

// WithClock returns a Option that sets the clock used by the time series.
func WithClock(c Clock) Option {
	return func(o *options) {
		o.clock = c
	}
}

// WithGranularities returns a Option that sets the granularites used by the time series.
func WithGranularities(g ...Granularity) Option {
	return func(o *options) {
		o.granularities = g
	}
}

type timeSeries struct {
	*sync.Mutex
	clock       Clock
	levels      []level
	pending     float64
	pendingTime time.Time
	latest      time.Time
}

// NewTimeSeries creates a new time series with the provided options.
// If no options are provided default values are used.
func NewTimeSeries(os ...Option) (TimeSeries, error) {
	opts := options{}
	for _, o := range os {
		o(&opts)
	}
	if opts.clock == nil {
		opts.clock = &defaultClock{}
	}
	if opts.granularities == nil {
		return nil, ErrNilGranularities
	}
	return newTimeSeries(opts.clock, opts.granularities)
}

func newTimeSeries(clock Clock, granularities []Granularity) (*timeSeries, error) {
	err := checkGranularities(granularities)
	if err != nil {
		return nil, err
	}
	return &timeSeries{clock: clock, levels: createLevels(clock, granularities), Mutex: &sync.Mutex{}}, nil
}

func checkGranularities(granularities []Granularity) error {
	if len(granularities) == 0 {
		return ErrBadGranularities
	}
	last := time.Duration(0)
	for i := 0; i < len(granularities); i++ {
		if granularities[i].Count == 0 {
			return ErrBadGranularities
		}
		if granularities[i].Granularity <= last {
			return ErrBadGranularities
		}
		last = granularities[i].Granularity
	}
	return nil
}

func createLevels(clock Clock, granularities []Granularity) []level {
	levels := make([]level, len(granularities))
	for i := range granularities {
		levels[i] = newLevel(clock, granularities[i].Granularity, granularities[i].Count)
	}
	return levels
}

// Increase adds amount at current time.
func (t *timeSeries) Add(amount float64) {
	t.AddAt(amount, t.clock.Now())
}

// All returns all non-nill values in bucket with biggest precision of the timeSeries
func (t *timeSeries) All() []PointValue {
	t.advance(t.clock.Now())
	for i := range t.levels {
		return t.levels[i].interval(t.levels[i].earliest(), t.levels[i].latest(), t.latest)
	}
	return []PointValue{}
}

// IncreaseAtTime adds amount at a specific time.
func (t *timeSeries) AddAt(amount float64, time time.Time) {
	if time.After(t.latest) {
		// fmt.Println("1st case")
		t.latest = time
	}
	if time.After(t.pendingTime) {
		// fmt.Println("2nd case")
		t.advance(time)
		t.pending = amount
	} else if time.After(t.pendingTime.Add(-t.levels[0].granularity)) {
		// fmt.Println("3rd case")
		t.pending = amount
	} else {
		// fmt.Println("4th case")
		t.increaseAtTime(amount, time)
	}
}

func (t *timeSeries) increaseAtTime(amount float64, time time.Time) {
	for i := range t.levels {
		if time.Before(t.levels[i].latest().Add(-1 * t.levels[i].duration())) {
			continue
		}
		t.levels[i].increaseAtTime(amount, time)
	}
}

func (t *timeSeries) advance(target time.Time) {
	// we need this here because advance is called from other locations
	// than IncreaseAtTime that don't check by themselves
	if !target.After(t.pendingTime) {
		return
	}
	t.advanceLevels(target)
	t.handlePending()
}

func (t *timeSeries) advanceLevels(target time.Time) {
	for i := range t.levels {
		if !target.Before(t.levels[i].latest().Add(t.levels[i].duration())) {
			t.levels[i].clear(target)
			continue
		}
		t.levels[i].advance(target)
	}
}

func (t *timeSeries) handlePending() {
	t.increaseAtTime(t.pending, t.pendingTime)
	t.pending = 0
	t.pendingTime = t.levels[0].latest()
}

// Recent returns the last value inserted
func (t *timeSeries) Last() (*PointValue, error) {
	if t.clock.Now().After(t.pendingTime) {
		t.advance(t.clock.Now())
	}
	for i := range t.levels {
		res := t.levels[i].last()
		if res != nil {
			return &PointValue{Time: t.latest, Value: *res}, nil
		}
	}
	return nil, errors.New("No last value in ts")
}

// RangeValues returns the values over the given range [start, end).
// ErrBadRange is returned if start is after end.
// ErrRangeNotCovered is returned if the range lies outside the time series.
func (t *timeSeries) Range(start, end time.Time) ([]PointValue, error) {
	if start.After(end) {
		return nil, ErrBadRange
	}
	t.advance(t.clock.Now())
	if ok, err := t.intersects(start, end); !ok {
		return nil, err
	}
	for i := range t.levels {
		// use !start.Before so earliest() is included
		// if we use earliest().Before() we won't get start
		if !start.Before(t.levels[i].earliest()) {
			return t.levels[i].interval(start, end, t.latest), nil
		}
	}
	return t.levels[len(t.levels)-1].interval(start, end, t.latest), nil
}

func (t *timeSeries) intersects(start, end time.Time) (bool, error) {
	biggestLevel := t.levels[len(t.levels)-1]
	if end.Before(biggestLevel.latest().Add(-biggestLevel.duration())) {
		return false, ErrRangeNotCovered
	}
	if start.After(t.levels[0].latest()) {
		return false, ErrRangeNotCovered
	}
	return true, nil
}
