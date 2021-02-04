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

	cl.Lock()
	_, err := cl.InstallContinuousQuery(
		`Max(Select("nr_goroutines","*"),"*")`,
		"the max of series nr_goroutines",
		expressionTimeout,
		exportFrequency,
		"nr_goroutines_max",
		defaultMetricCount,
		maxRetries,
	)
	cl.Unlock()

	if err != nil {
		panic(err)
	}

	cl.Lock()
	_, err = cl.InstallContinuousQuery(
		`Avg(Select("nr_goroutines","*"),"*")`,
		"the rolling average of series nr_goroutines",
		expressionTimeout,
		exportFrequency,
		"nr_goroutines_avg",
		defaultMetricCount,
		maxRetries,
	)
	cl.Unlock()

	if err != nil {
		panic(err)
	}

	cl.Lock()
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
	cl.Unlock()

	cl.Lock()
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
	cl.Unlock()

	cl.Lock()
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
	cl.Unlock()

	for range time.NewTicker(tickerTimeout).C {

		cl.Lock()
		res, err := cl.Query(
			"Select('nr_goroutines_neigh','*')",
			1*time.Second,
		)
		cl.Unlock()
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('nr_goroutines_neigh','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}

		cl.Lock()
		res, err = cl.Query(
			"Select('neigh_routines_min','*')",
			1*time.Second,
		)
		cl.Unlock()
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('neigh_routines_min','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}

		cl.Lock()
		res, err = cl.Query(
			"Select('nr_goroutines_avg','*')",
			1*time.Second,
		)
		cl.Unlock()
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

	cl.Lock()
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
	cl.Unlock()
	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {
		cl.Lock()
		res, err := cl.Query(
			"SelectLast('avg_nr_goroutines_global','*')",
			1*time.Second,
		)
		cl.Unlock()
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

	cl.Lock()
	res, err, _, updateChan := cl.SubscribeNodeUpdates()
	cl.Unlock()

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

	cl.Lock()
	msgChan, _, err := cl.InstallBroadcastMessageHandler("1")
	cl.Unlock()

	if err != nil {
		panic(err)
	}

	go func() {
		for range time.NewTicker(tickerTimeout).C {
			cl.Lock()
			err := cl.BroadcastMessage(
				body_types.Message{
					ID:      "1",
					TTL:     2,
					Content: struct{ Message string }{Message: "hey"}},
			)
			cl.Unlock()
			if err != nil {
				panic(err)
			}
		}
	}()

	for msg := range msgChan {
		fmt.Printf("Got message:%+v\n", msg)
	}
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
							lastPt = SelectLast("avg_nr_goroutines_global", "*")[0].Last()
							result = false
							if (lastPt.Value().count >= 12){
								result = true
							}`,
			},
			CheckPeriodic:      true,
			MaxRetries:         maxRetries,
			CheckPeriodicity:   checkFrequency,
			TriggerBackoffTime: 1 * time.Minute,
		})
	cl.Unlock()

	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case <-triggerChan:
				fmt.Print("------------ALARM TRIGGERED------------")
			case err := <-errChan:
				panic(fmt.Sprintf("alarm err: %s", err.Error()))
			case <-finishChan:
				panic("alarm finished channel was closed")
			}
		}
	}()
}

func testCustomInterestSets(cl *client.DemmonClient) {
	const (
		tickerTimeout     = 5 * time.Second
		exportFrequency   = 5 * time.Second
		expressionTimeout = 1 * time.Second
	)

	cl.Lock()
	setID, errChan, _, err := cl.InstallCustomInterestSet(body_types.CustomInterestSet{
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
	cl.Unlock()

	if err != nil {
		panic(err)
	}

	go func() {
		for err := range errChan {
			panic(err)
		}
	}()

	go func() {
		i := 0
		for range time.NewTicker(10 * time.Second).C {
			cl.Lock()
			var err error
			if i%2 == 0 {
				err = cl.UpdateCustomInterestSet(body_types.UpdateCustomInterestSetReq{
					SetID: *setID,
					Hosts: []body_types.CustomInterestSetHost{
						{
							IP:   demmonTreeConf.Landmarks[0].Peer.IP(),
							Port: 8090,
						},
					},
				})
			} else {
				err = cl.UpdateCustomInterestSet(body_types.UpdateCustomInterestSetReq{
					SetID: *setID,
					Hosts: []body_types.CustomInterestSetHost{},
				})
			}
			cl.Unlock()
			if err != nil {
				panic(err)
			}
			i++
		}
	}()

	for range time.NewTicker(tickerTimeout).C {

		cl.Lock()
		res, err := cl.Query(
			"Select('nr_goroutines_landmarks','*')",
			1*time.Second,
		)
		cl.Unlock()
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
		requestTimeout     = 3 * time.Second
	)

	clientConf := client.DemmonClientConf{
		DemmonPort:     8090,
		DemmonHostAddr: "localhost",
		RequestTimeout: requestTimeout,
	}
	cl := client.New(clientConf)

	var err error
	var errChan chan error
	for i := 0; i < 3; i++ {
		cl.Lock()
		err, errChan = cl.ConnectTimeout(connectTimeout)
		cl.Unlock()
		if err != nil {
			fmt.Println("failed to connect to demmon")
			time.Sleep(connectBackoffTime)
			continue
		}
		break
	}

	go func() {
		panic(<-errChan)
	}()

	if err != nil {
		panic("could not connect demmon client")
	} else {
		fmt.Println("CONNECTED TO DEMMON")
	}

	go testExporter(eConf)
	// go testGlobalAggFunc(cl)

	// go testAlarms(cl)
	// testNeighInterestSets(cl)
	go testCustomInterestSets(cl)
	// testGlobalAggFunc(cl)
	go testMsgBroadcast(cl)
}

func testExporter(eConf *exporter.Conf) {
	e, err, errChan := exporter.New(eConf, GetLocalIP().String(), "demmon", nil)
	if err != nil {
		panic(err)
	}

	go func() {
		panic(<-errChan)
	}()

	exportFrequncy := 5 * time.Second
	g := e.NewGauge("nr_goroutines", 12)
	setTicker := time.NewTicker(1 * time.Second)

	go e.ExportLoop(context.TODO(), exportFrequncy)

	for range setTicker.C {
		g.Set(float64(runtime.NumGoroutine()))
	}
}
