package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
)

var db = tsdb.GetDB("", "", false, false)

type mockClock struct {
	t time.Time
}

func (m *mockClock) Time() time.Time     { return m.t }
func (m *mockClock) Add(d time.Duration) { m.t = m.t.Add(d) }

var mockClockInstance = &mockClock{
	t: time.Time{},
}

func TestSelectQuery(t *testing.T) {
	clock := mockClockInstance

	tags := map[string]string{
		"tag1": "ola",
	}
	_, err := db.GetOrCreateTimeseriesWithClock("test", tags, mockClockInstance)

	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	val := map[string]interface{}{
		"val":  10,
		"val2": 20,
	}
	err = db.AddMetric("test", tags, val, clock.Time())

	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	clock.Add(1 * time.Second)
	err = db.AddMetric("test", tags, val, clock.Time())

	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	clock.Add(1 * time.Second)
	err = db.AddMetric("test", tags, val, clock.Time())

	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	me := NewMetricsEngine(db, Conf{}, false)
	script := `Select("test", "*")`
	// `timeseries = Select("test", {"tag1":"ola.*"})
	// 			for (i = 0; i < timeseries.length; i++) {
	// 				console.log(Object.getOwnPropertyNames(timeseries[i]))
	// 				points = timeseries[i].All()
	// 				for (i = 0; i < points.length; i++) {
	// 					console.log(points[i].Fields["val"])
	// 				}
	// 			}`

	v, err := me.MakeQuery(script, 3*time.Second)
	if err != nil {
		t.Error(err)
		t.FailNow()

		return
	}

	for _, ts := range v {
		fmt.Println(ts.All())
	}

	t.FailNow()
}

// func TestSelectAndMaxQuery(t *testing.T) {
// 	clock := clock.NewMock()

// 	tags := map[string]string{
// 		"tag1": "ola",
// 	}

// 	db.GetOrCreateTimeseriesWithClockAndGranularity("test", tags, clock, g)

// 	// db.AddMetric("cenas", make(map[string]string),)

//nolint:lll    // 	// err := db.AddMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
// 	// if err != nil {
// 	// 	t.Error(err)
// 	// 	t.FailNow()
// 	// }
// 	val := map[string]interface{}{}
// 	val["val"] = 10.0
// 	db.AddMetric("test", tags, val, clock.Now())
// 	clock.Add(1 * time.Second)
// 	me := NewMetricsEngine(db)
// 	script := `
// 	tsArr = select("test", {"tag1":"ola"})
// 	tsArr.map(function(ts) {
// 			console.log("adding point: ","test-max", JSON.stringify(ts.Tags()), JSON.stringify(max(ts, "val")))
// 			addPoint("test-max", ts.Tags(), max(ts, "val"))
// 		})
// 	`

// 	v, err := me.runWithTimeout(script, 1*time.Second)
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	fmt.Print(v.Export())
// 	t.FailNow()
// }

// func TestSelectAndAvgQuery(t *testing.T) {
// 	clock := clock.NewMock()

// 	tags := map[string]string{
// 		"tag1": "ola",
// 	}
// 	db.GetOrCreateTimeseriesWithClockAndGranularity("test", tags, clock, g)
// 	tags2 := map[string]string{
// 		"tag2": "ola",
// 	}
// 	db.GetOrCreateTimeseriesWithClockAndGranularity("test", tags2, clock, g)

// 	// db.AddMetric("cenas", make(map[string]string),)

//nolint:lll    // 	// err := db.AddMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
// 	// if err != nil {
// 	// 	t.Error(err)
// 	// 	t.FailNow()
// 	// }

// 	val := map[string]interface{}{}
// 	val["val"] = 10.0
// 	db.AddMetric("test", tags, val, clock.Now())

// 	val = map[string]interface{}{}
// 	val["val_2"] = 10.0
// 	db.AddMetric("test", tags2, val, clock.Now())

// 	clock.Add(1 * time.Second)
// 	val = map[string]interface{}{}
// 	val["val"] = 20.0
// 	db.AddMetric("test", tags, val, clock.Now())

// 	val = map[string]interface{}{}
// 	val["val_2"] = 10.0
// 	db.AddMetric("test", tags2, val, clock.Now())

// 	clock.Add(1 * time.Second)
// 	val = map[string]interface{}{}
// 	val["val"] = 45.0
// 	db.AddMetric("test", tags, val, clock.Now())

// 	val = map[string]interface{}{}
// 	val["val_2"] = 10.0
// 	db.AddMetric("test", tags2, val, clock.Now())
// 	clock.Add(1 * time.Second)
// 	me := NewMetricsEngine(db)
// 	script := `
// 	tsArr = select("test", "*")
// 	console.log(JSON.stringify(tsArr[0].All()))
// 	tsAvg = avg(tsArr, ".*")
// 	console.log(JSON.stringify(tsAvg.All()))`

// 	v, err := me.runWithTimeout(script, 1*time.Second)
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	fmt.Print(v.Export())
// 	t.FailNow()
// }

// func TestEvaluateNumericExpression(t *testing.T) {
// 	tsdb := tsdb.GetDB()
// 	// engine := New(tsdb)

// 	clock := clock.NewMock()
//nolint:lll    // 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}

// 	err = tsdb.AddMetricCurrTime("test", 13)
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}

// 	clock.Add(1 * time.Second)
// 	err = tsdb.AddMetricCurrTime("test", 15)
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	clock.Add(1 * time.Second)
// 	err = tsdb.AddMetricCurrTime("test", 17)
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	err = tsdb.AddMetricCurrTime("test", 14)
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	clock.Add(1 * time.Second)

// 	// err = tsdb.AddMetricCurrTime("test", toAdd)
// 	// if err != nil {
// 	// 	t.Error(err)
// 	// 	t.FailNow()
// 	// }

// 	// err = tsdb.AddMetricCurrTime("test", toAdd)
// 	// if err != nil {
// 	// 	t.Error(err)
// 	// 	t.FailNow()
// 	// }

// 	ts, err := tsdb.GetTimeseries("test")
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	val, err := ts.Last()
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}

// 	v, _ := ts.Range(clock.Now().Add(-3*time.Second), clock.Now())
// 	fmt.Printf("%+v", v)
// 	fmt.Printf("\nlast entry: %+v\n", val)

// 	// val, err := engine.evalNumericQuery("last('test')")
// 	// if err != nil {
// 	// 	t.Error(err)
// 	// 	t.Error(reflect.TypeOf(val))
// 	// 	t.FailNow()
// 	// }

// 	// if val != 13 {
// 	// 	t.Errorf("val should be: %f, but is: %f", toAdd, val)
// 	// 	t.FailNow()
// 	// }
// }

// func TestEvaluateNumericExpression2(t *testing.T) {
// 	tsdb := tsdb.GetDB()
// 	clock := clock.NewMock()
//nolint:lll    // 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	engine := New(tsdb)
// 	var toAdd float64 = 13
// 	err = tsdb.AddMetricCurrTime("test", toAdd)
// 	if err != nil {
// 		t.Error(err.Error())
// 		t.FailNow()
// 	}
// 	clock.Add(1 * time.Second)
// 	err = tsdb.AddMetricCurrTime("test", 12)
// 	if err != nil {
// 		t.Error(err.Error())
// 		t.FailNow()
// 	}

// 	clock.Add(1 * time.Second)
// 	err = tsdb.AddMetricCurrTime("test", 10)
// 	if err != nil {
// 		t.Error(err.Error())
// 		t.FailNow()
// 	}

// 	val, err := engine.evalNumericQuery("last('test') / 2")
// 	if err != nil {
// 		t.Error(err)
// 		t.Error(reflect.TypeOf(val))
// 		t.FailNow()
// 	}

// 	if val != 5.0 {
// 		t.Errorf("val should be: %f, but is: %f", 5.0, val)
// 		t.FailNow()
// 	}
// }

// func TestEvaluateNumericExpression3(t *testing.T) {
// 	tsdb := tsdb.GetDB()
// 	engine := New(tsdb)
// 	clock := clock.NewMock()
//nolint:lll    // 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	var toAdd float64 = 1
// 	tsdb.AddMetricCurrTime("test", toAdd)
// 	clock.Add(1 * time.Second)
// 	toAdd = 3
// 	tsdb.AddMetricCurrTime("test", toAdd)
// 	clock.Add(1 * time.Second)

// 	res, err := engine.evalNumericQuery("avg('test')")
// 	if err != nil {
// 		t.Error(err)
// 		t.Error(reflect.TypeOf(res))
// 		t.FailNow()
// 	}

// 	if res != 2 {
// 		t.Errorf("val should be: %f, but is: %f", toAdd, res)
// 		t.FailNow()
// 	}
// }

// func TestEvaluateNumericExpression4(t *testing.T) {
// 	tsdb := tsdb.GetDB()
// 	engine := New(tsdb)
// 	clock := clock.NewMock()
//nolint:lll    // 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}

// 	tsdb.AddMetricCurrTime("test", 3)
// 	clock.Add(1 * time.Second)
// 	tsdb.AddMetricCurrTime("test", 1)
// 	clock.Add(1 * time.Second)

// 	res, err := engine.evalNumericQuery("min('test')")
// 	if err != nil {
// 		t.Error(err)
// 		t.Error(reflect.TypeOf(res))
// 		t.FailNow()
// 	}

// 	if res != 1 {
// 		t.Errorf("val should be: %f, but is: %f", 1.0, res)
// 		t.FailNow()
// 	}
// }

// func TestEvaluateNumericExpression5(t *testing.T) {
// 	tsdb := tsdb.GetDB()
// 	engine := New(tsdb)
// 	clock := clock.NewMock()
//nolint:lll    // 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	tsdb.AddMetricCurrTime("test", 1)
// 	clock.Add(1 * time.Second)
// 	tsdb.AddMetricCurrTime("test", 3)
// 	clock.Add(1 * time.Second)

// 	res, err := engine.evalNumericQuery("max('test')")
// 	if err != nil {
// 		t.Error(err)
// 		t.Error(reflect.TypeOf(res))
// 		t.FailNow()
// 	}

// 	if res != 3 {
// 		t.Errorf("val should be: %f, but is: %f", 3.0, res)
// 		t.FailNow()
// 	}
// }

// func TestEvaluateNumericExpression6(t *testing.T) {
// 	tsdb := tsdb.GetDB()
// 	engine := New(tsdb)
// 	clock := clock.NewMock()
//nolint:lll    // 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	tsdb.AddMetricCurrTime("test", 1)
// 	clock.Add(1 * time.Second)
// 	tsdb.AddMetricCurrTime("test", 3)
// 	clock.Add(1 * time.Second)

// 	end := clock.Now()
// 	start := end.Add(-2 * time.Second).Add(500 * time.Millisecond)
// 	query := fmt.Sprintf("avg(range('test', %d, %d))", start.UnixNano(), end.UnixNano())

// 	t.Log(query)

// 	res, err := engine.evalNumericQuery(query)
// 	if err != nil {
// 		t.Error(err)
// 		t.Error(reflect.TypeOf(res))
// 		t.FailNow()
// 	}

// 	if res != 2 {
// 		t.Errorf("val should be: %f, but is: %f", 2.0, res)
// 		t.FailNow()
// 	}
// 	t.FailNow()
// }

// func TestEvaluateBoolExpression(t *testing.T) {
// 	tsdb := tsdb.GetDB()
// 	engine := New(tsdb)
// 	granularities := timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10})
// 	err := tsdb.RegisterMetric("test", granularities)
// 	if err != nil {
// 		t.Error(err)
// 		t.FailNow()
// 	}
// 	var toAdd float64 = 13
// 	tsdb.AddMetricCurrTime("test", toAdd)
// 	val, err := engine.evalBoolQuery("last('test') / 2 < 8")
// 	if err != nil {
// 		t.Error(err)
// 		t.Error(reflect.TypeOf(val))
// 		t.FailNow()
// 	}
// 	if !val {
// 		t.Errorf("val is not %t, (%t)", true, val)
// 		t.FailNow()
// 	}
// }
