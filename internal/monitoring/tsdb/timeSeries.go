package tsdb

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	ErrStartAfterFinish = errors.New("start is after finish")
)

type TimeSeries interface {
	Name() string
	SetName(name string)
	Tags() map[string]string
	SetTag(key, val string)
	All() []Observable
	Range(start time.Time, end time.Time) ([]Observable, error)
	AddPoint(p Observable)
	Last() Observable
	Frequency() time.Duration
	Count() int
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
	logger      *logrus.Entry
}

// NewTimeSeries creates a new TimeSeries using the function provided for creating new Observable.
func NewTimeSeries(name string, tags map[string]string, timeSeriesResolution time.Duration, tsLength int, logger *logrus.Entry) TimeSeries {
	return NewTimeSeriesWithClock(name, tags, timeSeriesResolution, tsLength, defaultClockInstance, logger)
}

// NewTimeSeriesWithClock creates a new TimeSeries using the function provided for creating new Observable and the clock for
// assigning timestamps.
func NewTimeSeriesWithClock(
	name string,
	tags map[string]string,
	timeSeriesResolution time.Duration,
	tsLength int,
	clock Clock,
	logger *logrus.Entry,
) TimeSeries {
	ts := new(timeSeries)
	ts.init(name, tags, timeSeriesResolution, tsLength, clock, logger)
	return ts
}

// Clear removes all observations from the time series.
func (ts *timeSeries) Clear() {
	ts.mu.Lock()
	ts.lastAdd = time.Time{}
	ts.pending = nil
	ts.pendingTime = time.Time{}
	ts.dirty = false
	ts.level.Clear()
	ts.mu.Unlock()
}

// Add records an observation at the current time.
func (ts *timeSeries) String() string {
	var sb = strings.Builder{}

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
	ts.addWithTime(observation, observation.TS())
	ts.mu.Unlock()
}

func (ts *timeSeries) SetName(newName string) {
	panic("should never set name of a mutable timeseries")
}

func (ts *timeSeries) SetTag(key, val string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.tags == nil {
		ts.tags = make(map[string]string)
	}

	ts.tags[key] = val
}

// ComputeRange computes a specified number of values into a slice using
// the observations recorded over the specified time period.
func (ts *timeSeries) Range(start, finish time.Time) ([]Observable, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if start.After(finish) {
		ts.logger.Infof("timeseries: start > finish, %v>%v", start, finish)
		return nil, ErrStartAfterFinish
	}

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
	// ts.logger.Info("Getting all points..")

	for i := 0; i < ts.numBuckets; i++ {
		idx := (i + ts.level.oldest) % ts.numBuckets
		// ts.logger.Infof("idx: %d; ts.level.bucket[idx]: %+v", idx, ts.level.bucket[idx])

		if ts.level.bucket[idx] != nil {
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
	toReturn := make(map[string]string, len(ts.tags))

	for k, v := range ts.tags {
		toReturn[k] = v
	}

	return toReturn
}

func (ts *timeSeries) Last() Observable {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	var result Observable = nil

	now := ts.clock.Time()

	if ts.level.end.Before(now) {
		ts.advance(now)
	}

	// ts.logger.Info("Getting last point...")

	ts.mergePendingUpdates()
	// ts.logger.Infof("ts.level.newest: %d", ts.level.newest)
	var idx int

	for i := 0; i < ts.numBuckets; i++ {

		idx = ts.level.newest - i
		if idx < 0 {
			idx += ts.numBuckets
		}

		// ts.logger.Infof("idx: %d; ts.level.bucket[idx]: %+v", idx, ts.level.bucket[idx])

		if ts.level.bucket[idx] != nil {
			result = ts.level.bucket[idx].Clone()
			break
		}
	}
	// ts.logger.Infof("idx: %d, Last point: %+v", idx, result)
	return result
}

// init initializes a level according to the supplied criteria.
func (ts *timeSeries) init(name string,
	tags map[string]string,
	resolution time.Duration,
	numBuckets int,
	clock Clock,
	logger *logrus.Entry) {
	ts.mu = sync.Mutex{}
	ts.name = name
	ts.tags = tags
	ts.numBuckets = numBuckets
	ts.clock = clock
	ts.level = &tsLevel{}
	newLevel := new(tsLevel)
	newLevel.InitLevel(resolution, ts.numBuckets)
	ts.level = newLevel
	ts.logger = logger
	ts.Clear()
}

// AddWithTime records an observation at the specified time.
func (ts *timeSeries) addWithTime(observation Observable, t time.Time) {

	ts.logger.Infof("Adding %+v at time %s", observation, t)

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
	if index > 0 && index < ts.numBuckets {
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
		ts.pending = nil
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
		for idx := range level.bucket {
			level.bucket[idx] = nil
		}

		level.end = time.Unix(0, (t.UnixNano()/level.size.Nanoseconds())*level.size.Nanoseconds())
	}

	for t.After(level.end) {
		level.end = level.end.Add(level.size)
		level.newest = level.oldest
		level.oldest = (level.oldest + 1) % ts.numBuckets
		level.bucket[level.newest] = nil
	}

	t = level.end
}

// latestBuckets returns a copy of the num latest buckets from level.
func (ts *timeSeries) latestBuckets(num int) []Observable {
	if num < 0 || num >= ts.numBuckets {
		ts.logger.Print("timeseries: bad num argument: ", num)
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
		if l.bucket[index] != nil {
			results[i] = l.bucket[index].Clone()
			// result.CopyFrom(l.bucket[index])
		}

		if index == 0 {
			index = ts.numBuckets
		}
		index--
	}

	return results
}

// extract returns a slice of specified number of observations from a given
// level over a given range.
func (ts *timeSeries) extract(l *tsLevel, start, finish time.Time) []Observable {
	ts.mergePendingUpdates()

	srcInterval := l.size
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
// func (ts *timeSeries) resetObservation(observation Observable) Observable {
// 	if observation == nil {
// 		observation = &observable{}
// 	} else {
// 		observation.Clear()
// 	}

// 	return observation
// }

// func minTime(a, b time.Time) time.Time {
// 	if a.Before(b) {
// 		return a
// 	}
// 	return b
// }

// func maxTime(a, b time.Time) time.Time {
// 	if a.After(b) {
// 		return a
// 	}
// 	return b
// }

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
// 	return &timeseries{measurementName: measurementName,
//	tags: tags,
//	clock: clock,
//	pending: &PointValue{TS: time.Time{}},
//	level: createLevel(clock, g), g: g, mu: &sync.RWMutex{}}
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
