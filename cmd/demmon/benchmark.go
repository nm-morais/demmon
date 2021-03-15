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

func benchmarkDemmonMetrics(eConf *exporter.Conf, isLandmark bool) {
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

	go setupBenchmarkExporter(eConf)
	if isLandmark {
		benchmarkTreeAggFunc(cl)
	}
}

func setupBenchmarkExporter(eConf *exporter.Conf) {
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

func benchmarkTreeAggFunc(cl *client.DemmonClient) {
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
		queryBackoff       = 3 * time.Second
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
			queryBackoff,
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
