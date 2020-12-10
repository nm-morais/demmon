package tsdb

import "time"

// Information kept per level. Each level consists of a circular list of
// observations. The start of the level may be derived from end and the
// len(buckets) * sizeInMillis.
type tsLevel struct {
	oldest int           // index to oldest bucketed Observable
	newest int           // index to newest bucketed Observable
	end    time.Time     // end timestamp for this level
	size   time.Duration // duration of the bucketed Observable
	bucket []Observable  // collections of observations
}

func (l *tsLevel) Clear() {
	l.oldest = 0
	l.newest = len(l.bucket) - 1
	l.end = time.Time{}
	for i := range l.bucket {
		if l.bucket[i] != nil {
			l.bucket[i] = nil
		}
	}
}

func (l *tsLevel) InitLevel(size time.Duration, nrSamples int) {
	l.size = size
	l.bucket = make([]Observable, nrSamples)
}

// type level struct {
// 	clock       Clock
// 	granularity time.Duration
// 	length      int
// 	end         time.Time
// 	oldest      int
// 	newest      int
// 	buckets     []map[string]interface{}
// }

// func newLevel(clock Clock, granularity time.Duration, length int) level {
// 	level := level{clock: clock, granularity: granularity, length: length}
// 	level.init()
// 	return level
// }

// func (l *level) init() {
// 	buckets := make([]map[string]interface{}, l.length)
// 	l.buckets = buckets
// 	l.clear(time.Time{})
// }

// func (l *level) clear(time time.Time) {
// 	l.oldest = 1
// 	l.newest = 0
// 	l.end = time.Truncate(l.granularity)
// 	for i := range l.buckets {
// 		l.buckets[i] = nil
// 	}
// }

// func (l *level) duration() time.Duration {
// 	return l.granularity*time.Duration(l.length) - l.granularity
// }

// func (l *level) earliest() time.Time {
// 	return l.end.Add(-l.duration())
// }

// func (l *level) latest() time.Time {
// 	return l.end
// }

// func (l *level) last() map[string]interface{} {
// 	// if l.buckets[(l.newest+1)%l.length] == nil {
// 	// panic(fmt.Sprint("BUCKETS:", l.buckets, "newest: ", l.newest, " oldest: ", l.oldest))
// 	// }
// 	// fmt.Println("BUCKETS:", l.buckets, "newest: ", l.newest, " oldest: ", l.oldest)
// 	idx := l.newest - 1
// 	if idx < 0 {
// 		idx += l.length
// 	}
// 	return l.buckets[idx]
// }

// func (l *level) addAtTime(v map[string]interface{}, time time.Time) {
// 	// fmt.Printf("adding at time %+v\n", time)
// 	difference := l.end.Sub(time.Truncate(l.granularity))
// 	if difference < 0 {
// 		// this cannot be negative because we advance before
// 		// can at least be 0
// 		log.Println("level.increaseAtTime was called with a time in the future")
// 	}
// 	// l.length-1 because the newest element is always l.length-1 away from oldest
// 	steps := (l.length - 1) - int(difference/l.granularity)
// 	index := (l.oldest + steps) % l.length
// 	fmt.Printf("Steps: %d, index: %d, l.oldest: %d, l.length: %d, l.granularity : %d\n", steps, index, l.oldest, l.length, l.granularity)
// 	l.buckets[index] = v
// }

// func (l *level) advance(target time.Time) {
// 	if !l.end.Before(target) {
// 		return
// 	}

// 	for target.After(l.end) {
// 		l.end = l.end.Add(l.granularity)
// 		l.buckets[l.oldest] = nil
// 		l.newest = l.oldest
// 		l.oldest = (l.oldest + 1) % len(l.buckets)
// 	}
// }

// func (l *level) interval(start, end time.Time, latest time.Time) []PointValue {
// 	if start.Before(l.earliest()) {
// 		start = l.earliest()
// 	}
// 	if end.After(l.latest()) {
// 		end = l.latest()
// 	}
// 	idx := 0
// 	// this is how many time steps start is away from earliest
// 	startSteps := start.Sub(l.earliest()) / l.granularity
// 	idx += int(startSteps)

// 	currentTime := l.earliest()
// 	currentTime = currentTime.Add(startSteps * l.granularity)

// 	res := make([]PointValue, 0)
// 	for idx < l.length && currentTime.Before(end) {
// 		nextTime := currentTime.Add(l.granularity)
// 		if nextTime.After(latest) {
// 			nextTime = latest
// 		}
// 		if nextTime.Before(start) {
// 			// the case nextTime.Before(start) happens when start is after latest
// 			// therefore we don't have data and can return
// 			break
// 		}

// 		if l.buckets[(l.oldest+idx)%l.length] != nil {
// 			v := l.buckets[(l.oldest+idx)%l.length]
// 			res = append(res, PointValue{TS: currentTime, Fields: v})
// 		}
// 		idx++
// 		currentTime = currentTime.Add(l.granularity)
// 	}
// 	return res
// }
