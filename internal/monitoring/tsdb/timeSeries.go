package tsdb

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type observable struct {
	ts     time.Time
	fields map[string]interface{}
}

func NewObservable(fields map[string]interface{}, ts time.Time) Observable {
	return &observable{
		ts:     ts,
		fields: fields,
	}
}

// Value returns the float's value.
func (f *observable) Value() map[string]interface{} {
	toReturn := make(map[string]interface{}, len(f.fields))
	for k, v := range f.fields {
		toReturn[k] = v
	}
	return toReturn
}

func (f *observable) Clone() Observable {
	toReturn := NewObservable(f.Value(), f.TS())
	return toReturn
}

func (f *observable) TS() time.Time {
	return f.ts
}

func (f *observable) Clear() {
	for k := range f.fields {
		delete(f.fields, k)
	}
}

func (f *observable) CopyFrom(other Observable) {
	// fmt.Println("CopyFrom")
	otherFields := other.Value()
	f.fields = make(map[string]interface{}, len(otherFields))
	for k, v := range otherFields {
		f.fields[k] = v
	}
	f.ts = other.TS()
}

func (f *observable) String() string {
	if f == nil {
		return "<nil>"
	}

	if len(f.fields) == 0 {
		return "<empty field>"
	}

	var sb strings.Builder
	sb.WriteString("[")
	for fieldKey, field := range f.fields {
		sb.WriteString(fmt.Sprintf("%s:%+v, ", fieldKey, field))
	}
	sb.WriteString("]")
	return sb.String()
}

// An Observable is a kind of data that can be aggregated in a time series.
type Observable interface {
	Clear()                    // Clears the observation so it can be reused.
	CopyFrom(other Observable) // Copies the contents of a given observation to self
	TS() time.Time
	Value() map[string]interface{}
	Clone() Observable
	String() string
}

type TimeSeries interface {
	Name() string
	Tags() map[string]string
	SetTag(key, val string)
	All() []Observable
	Range(start time.Time, end time.Time) ([]Observable, error)
	AddPoint(p Observable)
	Last() Observable
	Frequency() time.Duration
	Count() int
	MarshalJSON() ([]byte, error)
	Clear()
}

// A Clock tells the current time.
type Clock interface {
	Time() time.Time
}

type defaultClock int

var defaultClockInstance defaultClock

func (defaultClock) Time() time.Time { return time.Now() }

// Keeps a sequence of levels. Each level is responsible for storing data at
// a given resolution. For example, the first level stores data at a one
// minute resolution while the second level stores data at a one hour
// resolution.

// Each level is represented by a sequence of buckets. Each bucket spans an
// interval equal to the resolution of the level. New observations are added
// to the last bucket.
type timeSeries struct {
	mu          sync.Mutex
	numBuckets  int        // number of buckets in each level
	level       *tsLevel   // levels of bucketed Observable
	lastAdd     time.Time  // time of last Observable tracked
	clock       Clock      // Clock for getting current time
	pending     Observable // observations not yet bucketed
	pendingTime time.Time  // what time are we keeping in pending
	dirty       bool       // if there are pending observations
	tags        map[string]string
	name        string
}

// NewTimeSeries creates a new TimeSeries using the function provided for creating new Observable.
func NewTimeSeries(name string, tags map[string]string, timeSeriesResolution time.Duration, tsLength int) TimeSeries {
	return NewTimeSeriesWithClock(name, tags, timeSeriesResolution, tsLength, defaultClockInstance)
}

// NewTimeSeriesWithClock creates a new TimeSeries using the function provided for creating new Observable and the clock for
// assigning timestamps.
func NewTimeSeriesWithClock(name string, tags map[string]string, timeSeriesResolution time.Duration, tsLength int, clock Clock) TimeSeries {
	ts := new(timeSeries)
	ts.init(name, tags, timeSeriesResolution, tsLength, clock)
	return ts
}

// Clear removes all observations from the time series.
func (ts *timeSeries) Clear() {
	ts.mu.Lock()
	ts.lastAdd = time.Time{}
	ts.pending = ts.resetObservation(ts.pending)
	ts.pendingTime = time.Time{}
	ts.dirty = false
	ts.level.Clear()
	ts.mu.Unlock()
}

// Add records an observation at the current time.
func (ts *timeSeries) String() string {
	// ts.mu.Lock()
	// defer ts.mu.Unlock()
	var sb strings.Builder = strings.Builder{}
	sb.WriteString(fmt.Sprintf("Name: %s", ts.name))
	sb.WriteString(" | Tags: ")
	sb.WriteString("[")
	for tagKey, tagVal := range ts.tags {
		sb.WriteString(fmt.Sprintf("%s:%s, ", tagKey, tagVal))
	}
	sb.WriteString("]")
	sb.WriteString(" | Fields: ")
	for _, field := range ts.All() {
		if field != nil {
			sb.WriteString(field.String())
		}
	}
	return sb.String()
}

// Add records an observation at the current time.
func (ts *timeSeries) AddPoint(observation Observable) {
	ts.mu.Lock()
	ts.addWithTime(observation, ts.clock.Time())
	ts.mu.Unlock()
}

func (t *timeSeries) SetTag(key, val string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tags == nil {
		t.tags = make(map[string]string)
	}
	t.tags[key] = val
}

// ComputeRange computes a specified number of values into a slice using
// the observations recorded over the specified time period.
func (ts *timeSeries) Range(start, finish time.Time) ([]Observable, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if start.After(finish) {
		log.Printf("timeseries: start > finish, %v>%v", start, finish)
		return nil, errors.New("Start is after finish")
	}

	// if num < 0 {
	// 	log.Printf("timeseries: num < 0, %v", num)
	// 	return nil
	// }

	l := ts.level
	if !start.Before(l.end.Add(-l.size * time.Duration(ts.numBuckets))) {
		results := ts.extract(l, start, finish)
		return results, nil
	}

	// Failed to find a level that covers the desired range. So just
	// extract from the last level, even if it doesn't cover the entire
	// desired range.
	// ts.extract(ts.level, start, finish, results)
	return nil, nil
}

func (ts *timeSeries) All() []Observable {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	now := ts.clock.Time()
	if ts.level.end.Before(now) {
		ts.advance(now)
	}
	ts.mergePendingUpdates()
	results := make([]Observable, 0, ts.numBuckets)
	for i := 0; i < ts.numBuckets; i++ {
		idx := (i + ts.level.oldest) % ts.numBuckets
		if ts.level.bucket[idx] != nil {
			// fmt.Printf("i:%d, idx:%d, value: %+v\n", i, idx, ts.level.bucket[idx])
			srcValue := ts.level.bucket[idx]
			results = append(results, srcValue.Clone())
		}
	}
	return results
}

func (ts *timeSeries) Frequency() time.Duration {
	return ts.level.size
}

func (ts *timeSeries) Count() int {
	return ts.numBuckets
}

func (ts *timeSeries) Name() string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.name
}

func (ts *timeSeries) Tags() map[string]string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	toReturn := make(map[string]interface{}, len(ts.tags))
	for k, v := range ts.tags {
		toReturn[k] = v
	}
	return ts.tags
}

func (ts *timeSeries) Last() Observable {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	// aux := ts.latestBuckets(1)
	// if len(aux) > 0 {
	// 	return aux[0]
	// }
	var result Observable
	now := ts.clock.Time()
	if ts.level.end.Before(now) {
		ts.advance(now)
	}
	ts.mergePendingUpdates()
	l := ts.level
	index := l.newest
	for i := 0; i < ts.numBuckets; i++ {
		if l.bucket[index] != nil {
			result = l.bucket[index].Clone()
			if result == nil {
				panic("Cloned result is nil")
			}
			break
		}
		if index == 0 {
			index = ts.numBuckets
		}
		index -= 1
	}
	return result
}

// init initializes a level according to the supplied criteria.
func (ts *timeSeries) init(name string, tags map[string]string, resolution time.Duration, numBuckets int, clock Clock) {
	ts.mu = sync.Mutex{}
	ts.name = name
	ts.tags = tags
	ts.numBuckets = numBuckets
	ts.clock = clock
	ts.level = &tsLevel{}
	newLevel := new(tsLevel)
	newLevel.InitLevel(resolution, ts.numBuckets)
	ts.level = newLevel
	ts.Clear()
}

// AddWithTime records an observation at the specified time.
func (ts *timeSeries) addWithTime(observation Observable, t time.Time) {
	// fmt.Printf("Adding point %s at time %s\n", observation, t)
	// defer fmt.Printf("Done adding point %s at time %s\n", observation, t)
	if t.After(ts.lastAdd) {
		ts.lastAdd = t
	}
	if t.After(ts.pendingTime) {
		ts.advance(t)
		ts.mergePendingUpdates()
		ts.pendingTime = ts.level.end
		ts.pending = observation
		ts.dirty = true
	} else if t.After(ts.pendingTime.Add(-1 * ts.level.size)) {
		// The observation is close enough to go into the pending bucket.
		// This compensates for clock skewing and small scheduling delays
		// by letting the update stay in the fast path.
		ts.pending = observation
		ts.dirty = true
	} else {
		ts.mergeValue(observation, t)
	}
}

// mergeValue inserts the observation at the specified time in the past into all levels.
func (ts *timeSeries) mergeValue(observation Observable, t time.Time) {
	index := (ts.numBuckets - 1) - int(ts.level.end.Sub(t)/ts.level.size)
	if 0 <= index && index < ts.numBuckets {
		bucketNumber := (ts.level.oldest + index) % ts.numBuckets
		if ts.level.bucket[bucketNumber] == nil {
			ts.level.bucket[bucketNumber] = observation.Clone()
		}
		ts.level.bucket[bucketNumber] = observation.Clone()
	}
}

// mergePendingUpdates applies the pending updates into all levels.
func (ts *timeSeries) mergePendingUpdates() {
	if ts.dirty {
		ts.mergeValue(ts.pending, ts.pendingTime)
		ts.pending = ts.resetObservation(ts.pending)
		ts.dirty = false
	}
}

// advance cycles the buckets at each level until the latest bucket in
// each level can hold the time specified.
func (ts *timeSeries) advance(t time.Time) {
	if !t.After(ts.level.end) {
		return
	}

	level := ts.level
	if !level.end.Before(t) {
		return
	}

	// If the time is sufficiently far, just clear the level and advance
	// directly.
	if !t.Before(level.end.Add(level.size * time.Duration(ts.numBuckets))) {
		for _, b := range level.bucket {
			ts.resetObservation(b)
		}
		level.end = time.Unix(0, (t.UnixNano()/level.size.Nanoseconds())*level.size.Nanoseconds())
	}

	for t.After(level.end) {
		level.end = level.end.Add(level.size)
		level.newest = level.oldest
		level.oldest = (level.oldest + 1) % ts.numBuckets
		ts.resetObservation(level.bucket[level.newest])
	}

	t = level.end
}

// latestBuckets returns a copy of the num latest buckets from level.
func (ts *timeSeries) latestBuckets(num int) []Observable {
	if num < 0 || num >= ts.numBuckets {
		log.Print("timeseries: bad num argument: ", num)
		return nil
	}
	results := make([]Observable, num)
	now := ts.clock.Time()
	if ts.level.end.Before(now) {
		ts.advance(now)
	}
	ts.mergePendingUpdates()
	l := ts.level
	index := l.newest
	for i := 0; i < num; i++ {
		result := &observable{}
		results[i] = result
		if l.bucket[index] != nil {
			result.CopyFrom(l.bucket[index])
		}
		if index == 0 {
			index = ts.numBuckets
		}
		index -= 1
	}
	return results
}

func (t *timeSeries) MarshalJSON() ([]byte, error) {
	panic("not implemented")
}

// extract returns a slice of specified number of observations from a given
// level over a given range.
func (ts *timeSeries) extract(l *tsLevel, start, finish time.Time) []Observable {
	ts.mergePendingUpdates()

	srcInterval := l.size
	// dstInterval := finish.Sub(start) / time.Duration(num)
	dstStart := start
	srcStart := l.end.Add(-srcInterval * time.Duration(ts.numBuckets))
	srcIndex := 0

	// Where should scanning start?
	if dstStart.After(srcStart) {
		advance := int(dstStart.Sub(srcStart) / srcInterval)
		srcIndex += advance
		srcStart = srcStart.Add(time.Duration(advance) * srcInterval)
	}

	// The i'th value is computed as show below.
	// interval = (finish/start)/num
	// i'th value = sum of observation in range
	//   [ start + i       * interval,
	//     start + (i + 1) * interval )
	results := make([]Observable, 0, ts.numBuckets-srcIndex)
	for srcIndex < ts.numBuckets && srcStart.Before(finish) {
		srcEnd := srcStart.Add(srcInterval)
		if srcEnd.After(ts.lastAdd) {
			srcEnd = ts.lastAdd
		}

		if !srcEnd.Before(dstStart) {
			srcValue := l.bucket[(srcIndex+l.oldest)%ts.numBuckets]
			// if !srcStart.Before(dstStart) && !srcEnd.After(dstEnd) {
			// dst completely contains src.

			if srcValue != nil {
				results = append(results, srcValue.Clone())
			}

			// } else {
			// 	// dst partially overlaps src.
			// 	overlapStart := maxTime(srcStart, dstStart)
			// 	overlapEnd := minTime(srcEnd, dstEnd)
			// 	base := srcEnd.Sub(srcStart)
			// 	fraction := overlapEnd.Sub(overlapStart).Seconds() / base.Seconds()
			//
			// 	used := &PointValue{}
			// 	if srcValue != nil {
			// 		used.CopyFrom(srcValue)
			// 	}
			// 	used.Multiply(fraction)
			// 	results[i].Add(used)
			// }
			if srcEnd.After(finish) {
				break
			}
		}
		srcIndex++
		srcStart = srcStart.Add(srcInterval)
	}
	return results
}

// resetObservation clears the content so the struct may be reused.
func (ts *timeSeries) resetObservation(observation Observable) Observable {
	if observation == nil {
		observation = &observable{}
	} else {
		observation.Clear()
	}
	return observation
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// var (
// 	ErrNilGranularities = errors.New("timeseries: range is nil")
// 	// ErrBadRange indicates that the given range is invalid. Start should always be <= End
// 	ErrBadRange = errors.New("timeseries: range is invalid")
// 	// ErrBadGranularities indicates that the provided granularities are not strictly increasing
// 	ErrBadGranularities = errors.New("timeseries: granularities must be strictly increasing and non empty")

// 	ErrBadCount = errors.New("timeseries: granularities count must be > 1")

// 	// ErrRangeNotCovered indicates that the provided range lies outside the time series
// 	ErrRangeNotCovered = errors.New("timeseries: range is not convered")
// )

// type timeseries struct {
// 	measurementName string
// 	g               Granularity
// 	tags            map[string]string
// 	clock           Clock
// 	level           level
// 	pending         *PointValue
// 	latest          time.Time
// 	mu              *sync.RWMutex
// }

// func NewTimeSeries(measurementName string, tags map[string]string, g Granularity, clock Clock) TimeSeries {
// 	if clock == nil {
// 		clock = &defaultClock{}
// 	}
// 	return &timeseries{measurementName: measurementName, tags: tags, clock: clock, pending: &PointValue{TS: time.Time{}}, level: createLevel(clock, g), g: g, mu: &sync.RWMutex{}}
// }

// func checkGranularity(granularity Granularity) error {
// 	if granularity.Count == 0 {
// 		return ErrBadGranularities
// 	}

// 	if granularity.Count == 1 {
// 		return ErrBadGranularities
// 	}

// 	if granularity.Granularity == 0 {
// 		return ErrBadGranularities
// 	}
// 	return nil
// }

// func createLevel(clock Clock, g Granularity) level {
// 	return newLevel(clock, g.Granularity, g.Count)
// }

// func (t *timeseries) AddPoint(p PointValue) {
// 	t.mu.Lock()
// 	defer t.mu.Unlock()
// 	if p.TS.After(t.latest) {
// 		t.latest = p.TS
// 	}
// 	if p.TS.After(t.pending.TS) {
// 		t.advance(p.TS)
// 		t.pending.TS = p.TS
// 		t.pending.Fields = p.Fields
// 	} else if p.TS.After(t.pending.TS.Add(-t.level.granularity)) {
// 		t.pending.TS = p.TS
// 		t.pending.Fields = p.Fields
// 	} else {
// 		t.level.addAtTime(p.Fields, p.TS)
// 	}
// }

// func (t *timeseries) Name() string {
// 	return t.measurementName
// }

// func (t *timeseries) Tags() map[string]string {
// 	auxMap := make(map[string]string)
// 	for tName, tVal := range t.tags {
// 		auxMap[tName] = tVal
// 	}
// 	return auxMap
// }

// func (t *timeseries) All() []PointValue {
// 	t.mu.Lock()
// 	t.advance(t.clock.Now())
// 	t.mu.Unlock()
// 	t.mu.RLock()
// 	defer t.mu.RUnlock()
// 	return t.level.interval(t.level.earliest(), t.level.latest(), t.latest)
// }

// func (t *timeseries) Clear() {
// 	t.mu.Lock()
// 	defer t.mu.Unlock()
// 	t.level.clear(time.Now())
// }

// // Recent returns the last value inserted
// func (t *timeseries) Last() *PointValue {
// 	t.mu.Lock()
// 	defer t.mu.Unlock()
// 	if t.clock.Now().After(t.pending.TS) {
// 		t.advance(t.clock.Now())
// 	}
// 	res := t.level.last()
// 	if res != nil {
// 		return &PointValue{TS: t.latest, Fields: res}
// 	}
// 	return nil
// }

// func (t *timeseries) Range(start, end time.Time) ([]PointValue, error) {
// 	if start.After(end) {
// 		return nil, ErrBadRange
// 	}
// 	t.mu.Lock()
// 	t.advance(t.clock.Now())
// 	t.mu.Unlock()

// 	t.mu.RLock()
// 	defer t.mu.RUnlock()
// 	if ok, err := t.intersects(start, end); !ok {
// 		return nil, err
// 	}
// 	// use !start.Before so earliest() is included
// 	// if we use earliest().Before() we won't get start
// 	return t.level.interval(start, end, t.latest), nil
// }

// func (t *timeseries) Granularity() Granularity {
// 	return t.g
// }

// func (t *timeseries) handlePending() {
// 	t.setAtTime(t.pending.Fields, t.pending.TS)
// 	t.pending.Fields = nil
// 	t.pending.TS = t.level.latest()
// }

// func (t *timeseries) setAtTime(values map[string]interface{}, time time.Time) {
// 	if !time.Before(t.level.latest().Add(-1 * t.level.duration())) {
// 		t.level.addAtTime(values, time)
// 	}
// }

// func (t *timeseries) advance(target time.Time) {
// 	if !target.After(t.pending.TS) {
// 		return
// 	}
// 	t.advanceLevels(target)
// 	t.handlePending()
// }

// func (t *timeseries) advanceLevels(target time.Time) {
// 	if !target.Before(t.level.latest().Add(t.level.duration())) {
// 		t.level.clear(target)
// 	}
// 	t.level.advance(target)
// }

// func (t *timeseries) intersects(start, end time.Time) (bool, error) {
// 	if start.After(t.level.latest()) {
// 		return false, ErrRangeNotCovered
// 	}
// 	return true, nil
// }
