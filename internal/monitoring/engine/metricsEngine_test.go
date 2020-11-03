package engine

// func TestEvaluateNumericExpression(t *testing.T) {
// 	tsdb := tsdb.GetDB()
// 	// engine := New(tsdb)

// 	clock := clock.NewMock()
// 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
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
// 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
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
// 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
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
// 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
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
// 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
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
// 	err := tsdb.RegisterMetric("test", timeseries.WithClock(clock), timeseries.WithGranularities(timeseries.Granularity{Granularity: time.Second, Count: 10}))
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
