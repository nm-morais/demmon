package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	client "github.com/nm-morais/demmon-client/pkg"
	"github.com/nm-morais/demmon-common/body_types"
	exporter "github.com/nm-morais/demmon-exporter"
)

func testNeighInterestSets(cl *client.DemmonClient) {
	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 5 * time.Second
		defaultTTL         = 2
		defaultMetricCount = 12
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 5 * time.Second
		requestTimeout     = 1 * time.Second
	)

	_, err := cl.InstallContinuousQuery(
		`Max(Select("nr_goroutines","*"),"*")`,
		"the max of series nr_goroutines",
		expressionTimeout,
		exportFrequency,
		"nr_goroutines_max",
		defaultMetricCount,
		maxRetries,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallContinuousQuery(
		`Avg(Select("nr_goroutines","*"),"*")`,
		"the rolling average of series nr_goroutines",
		expressionTimeout,
		exportFrequency,
		"nr_goroutines_avg",
		defaultMetricCount,
		maxRetries,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallContinuousQuery(
		`Min(Select("nr_goroutines","*"),"*")`,
		"the min of series nr_goroutines",
		expressionTimeout,
		exportFrequency,
		"nr_goroutines_min",
		defaultMetricCount,
		maxRetries,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallNeighborhoodInterestSet(&body_types.NeighborhoodInterestSet{
		IS: body_types.InterestSet{
			MaxRetries: maxRetries,
			Query: body_types.RunnableExpression{
				Expression: `SelectLast("nr_goroutines","*")`,
				Timeout:    expressionTimeout,
			},
			OutputBucketOpts: body_types.BucketOptions{
				Name: "nr_goroutines_neigh",
				Granularity: body_types.Granularity{
					Granularity: exportFrequency,
					Count:       3,
				},
			},
		},
		TTL: defaultTTL,
	})

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallContinuousQuery(
		`neighRoutines = Select("nr_goroutines_neigh","*")
		 result = Min(neighRoutines, "*")`,
		"the min of nr_goroutines of the neighborhood",
		expressionTimeout,
		exportFrequency,
		"neigh_routines_min",
		defaultMetricCount,
		maxRetries,
	)

	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {

		res, err := cl.Query(
			"Select('nr_goroutines_neigh','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('nr_goroutines_neigh','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}

		res, err = cl.Query(
			"Select('neigh_routines_min','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('neigh_routines_min','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}

		res, err = cl.Query(
			"Select('nr_goroutines_avg','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('nr_goroutines_avg','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}
	}
}

func testTreeAggFunc(cl *client.DemmonClient) {

	// 	`neighRoutines = Select("nr_goroutines_neigh","*")
	// 	result = Min(neighRoutines, "*")`,
	//    "the min of nr_goroutines of the neighborhood",
	//    expressionTimeout,
	//    exportFrequency,
	//    "neigh_routines_min",
	//    defaultMetricCount,
	//    maxRetries,

	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 5 * time.Second
		defaultTTL         = 2
		defaultMetricCount = 12
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 5 * time.Second
		requestTimeout     = 1 * time.Second
	)

	_, err := cl.InstallTreeAggregationFunction(
		&body_types.TreeAggregationSet{
			MaxRetries: 3,
			Query: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `point = SelectLast("nr_goroutines","*")[0].Last()
							result = {"count":1, "value":point.Value().value}`,
			},
			OutputBucketOpts: body_types.BucketOptions{
				Name: "avg_nr_goroutines_tree",
				Granularity: body_types.Granularity{
					Granularity: exportFrequency,
					Count:       defaultMetricCount,
				},
			},
			MergeFunction: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `
							aux = {"count":0, "value":0}
							for (i = 0; i < args.length; i++) {
								aux.count += args[i].count
								aux.value += args[i].value					
							}
							result = aux
							`,
			},
			Levels: 10,
		})

	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {
		res, err := cl.Query(
			"Select('avg_nr_goroutines_tree','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('avg_nr_goroutines_tree','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}
	}
}

func testGlobalAggFunc(cl *client.DemmonClient) {

	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 5 * time.Second
		defaultMetricCount = 12
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 5 * time.Second
		requestTimeout     = 1 * time.Second
	)

	_, err := cl.InstallGlobalAggregationFunction(
		&body_types.GlobalAggregationFunction{
			MaxRetries: 3,
			Query: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `point = SelectLast("nr_goroutines","*")[0].Last()
							result = {"count":1, "value":point.Value().value}`,
			},
			OutputBucketOpts: body_types.BucketOptions{
				Name: "avg_nr_goroutines_global",
				Granularity: body_types.Granularity{
					Granularity: exportFrequency,
					Count:       defaultMetricCount,
				},
			},
			MergeFunction: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `
							aux = {"count":0, "value":0}
							for (i = 0; i < args.length; i++) {
								aux.count += args[i].count
								aux.value += args[i].value					
							}
							result = aux
							`,
			},
			DifferenceFunction: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `
							toSubtractFrom = args[0]
							for (i = 1; i < args.length; i++) {
								toSubtractFrom.count -= args[i].count
								toSubtractFrom.value -= args[i].value					
							}
							result = toSubtractFrom
							`,
			},
		})

	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {
		res, err := cl.Query(
			"SelectLast('avg_nr_goroutines_global','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("SelectLast('avg_nr_goroutines_global','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}
	}
}

func testNodeUpdates(cl *client.DemmonClient) {

	res, err, _, updateChan := cl.SubscribeNodeUpdates()

	if err != nil {
		panic(err)
	}

	fmt.Println("Starting view:", res)

	go func() {
		for nodeUpdate := range updateChan {
			fmt.Printf("Node Update: %+v\n", nodeUpdate)
		}
	}()
}

func testMsgBroadcast(cl *client.DemmonClient) {
	const tickerTimeout = 5 * time.Second

	msgChan, _, err := cl.InstallBroadcastMessageHandler(1)

	if err != nil {
		panic(err)
	}

	go func() {
		for range time.NewTicker(tickerTimeout).C {
			err := cl.BroadcastMessage(
				body_types.Message{
					ID:      1,
					TTL:     2,
					Content: struct{ Message string }{Message: "hey"}},
			)
			if err != nil {
				panic(err)
			}
		}
	}()

	go func() {
		for msg := range msgChan {
			fmt.Printf("Got message:%+v\n", msg)
		}
	}()
}

func testAlarms(cl *client.DemmonClient) {
	const (
		checkFrequency    = 5 * time.Second
		expressionTimeout = 1 * time.Second
		maxRetries        = 3
	)

	cl.Lock()
	_, triggerChan, errChan, finishChan, err := cl.InstallAlarm(
		&body_types.InstallAlarmRequest{
			WatchList: []body_types.TimeseriesFilter{{
				MeasurementName: "avg_nr_goroutines_global",
				TagFilters:      map[string]string{},
			}},
			Query: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `
							lastPt = SelectLast("avg_nr_goroutines_global")[0].Last()
							retult = False
							if (lastPt.Count >= 13){
								result = True
							}`,
			},
			CheckOnlyOnChange: false,
			MaxRetries:        maxRetries,
			CheckPeriodicity:  checkFrequency,
		})
	cl.Unlock()

	go func() {
		for {
			select {
			case <-triggerChan:
				fmt.Print("------------ALARM TRIGGERED------------")
			case <-errChan:
				panic(fmt.Sprintf("alarm err: %s", err))
			case <-finishChan:
				panic("alarm finished channel was closed")
			}
		}
	}()

	if err != nil {
		panic(err)
	}
}

func testCustomInterestSets(cl *client.DemmonClient) {
	const (
		tickerTimeout     = 5 * time.Second
		exportFrequency   = 5 * time.Second
		expressionTimeout = 1 * time.Second
	)
	_, errChan, _, err := cl.InstallCustomInterestSet(body_types.CustomInterestSet{
		DialTimeout:      3 * time.Second,
		DialRetryBackoff: 5 * time.Second,
		Hosts: []body_types.CustomInterestSetHost{
			{
				IP:   demmonTreeConf.Landmarks[0].Peer.IP(),
				Port: 8090,
			},
		},
		IS: body_types.InterestSet{MaxRetries: 3,
			Query: body_types.RunnableExpression{
				Timeout:    expressionTimeout,
				Expression: `SelectLast("nr_goroutines","*")`,
			},
			OutputBucketOpts: body_types.BucketOptions{
				Name: "nr_goroutines_landmarks",
				Granularity: body_types.Granularity{
					Granularity: exportFrequency,
					Count:       10,
				},
			},
		},
	})

	if err != nil {
		panic(err)
	}

	go func() {
		for err := range errChan {
			panic(err)
		}
	}()

	for range time.NewTicker(tickerTimeout).C {

		res, err := cl.Query(
			"Select('nr_goroutines_landmarks','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('nr_goroutines_landmarks','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}
	}
}

func testDemmonMetrics(eConf *exporter.Conf, isLandmark bool) {
	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 5 * time.Second
		defaultTTL         = 2
		defaultMetricCount = 5
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 5 * time.Second
		requestTimeout     = 1 * time.Second
	)

	clientConf := client.DemmonClientConf{
		DemmonPort:     8090,
		DemmonHostAddr: "localhost",
		RequestTimeout: requestTimeout,
	}
	cl := client.New(clientConf)

	for i := 0; i < 3; i++ {
		err := cl.ConnectTimeout(connectTimeout)
		if err != nil {
			time.Sleep(connectBackoffTime)
			continue
		}
		break
	}
	go testExporter(eConf)
	go testGlobalAggFunc(cl)
	go testAlarms(cl)

	// if isLandmark {
	// 	testGlobalAggFunc(cl)
	// }
}

func testExporter(eConf *exporter.Conf) {
	e, err := exporter.New(eConf, GetLocalIP().String(), "demmon", nil)
	if err != nil {
		panic(err)
	}

	exportFrequncy := 5 * time.Second
	g := e.NewGauge("nr_goroutines", 12)
	setTicker := time.NewTicker(1 * time.Second)

	go e.ExportLoop(context.TODO(), exportFrequncy)

	for range setTicker.C {
		g.Set(float64(runtime.NumGoroutine()))
	}
}
