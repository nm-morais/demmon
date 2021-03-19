package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"time"

	client "github.com/nm-morais/demmon-client/pkg"
	"github.com/nm-morais/demmon-common/body_types"
	exporter "github.com/nm-morais/demmon-exporter"
)

type BenchmarkType = string

const (
	BenchmarkTreeAggFunc   = BenchmarkType("BenchmarkTreeAggFunc")
	BenchmarkGlobalAggFunc = BenchmarkType("BenchmarkGlobalAggFunc")
)

func benchmarkDemmonMetrics(eConf *exporter.Conf, isLandmark bool, benchmarkType BenchmarkType) {
	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 3 * time.Second
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

	go setupBenchmarkExporter(eConf, logFolder, exportFrequency)

	switch benchmarkType {
	case BenchmarkTreeAggFunc:
		if isLandmark {
			benchmarkTreeAggFunc(cl, expressionTimeout, exportFrequency)
		}
	case BenchmarkGlobalAggFunc:
		benchmarkGlobalAggFunc(cl) // TODO copy argss from above
	}
}

func setupBenchmarkExporter(eConf *exporter.Conf, logFolder string, queryFrequency time.Duration) {
	localValuesCsvWriter := setupCSVWriter(logFolder, "/local-values.csv", []string{"dummy_value", "timestamp"})

	e, err, errChan := exporter.New(eConf, GetLocalIP().String(), "demmon", nil)
	if err != nil {
		panic(err)
	}

	exportFrequency := queryFrequency / 2
	g := e.NewGauge("dummy_value", 12)
	setTicker := time.NewTicker(exportFrequency / 2)
	writeLocalTicker := time.NewTicker(queryFrequency)

	go func() {
		for {
			select {
			case <-setTicker.C:
				g.Set(float64(1))
			case <-writeLocalTicker.C:
				writeOrPanic(localValuesCsvWriter, []string{fmt.Sprintf("%d", int(1)), fmt.Sprintf("%d", time.Now().UnixNano())})
			case err := <-errChan:
				panic(err)
			}
		}
	}()
	e.ExportLoop(context.TODO(), exportFrequency)
}

func writeOrPanic(csvWriter *csv.Writer, records []string) {
	if err := csvWriter.Write(records); err != nil {
		panic(err)
	}
	csvWriter.Flush()
}

func benchmarkTreeAggFunc(cl *client.DemmonClient, expressionTimeout, exportFrequency time.Duration) {
	csvWriter := setupCSVWriter(logFolder, "/results.csv", []string{"avg_dummy_value_tree", "timestamp"})

	_, err := cl.InstallTreeAggregationFunction(
		&body_types.TreeAggregationSet{
			MaxRetries: 3,
			Query: body_types.RunnableExpression{Timeout: expressionTimeout, Expression: `point = SelectLast("dummy_value","*")[0].Last()
											result = {"count":1, "value":point.Value().value}`},
			OutputBucketOpts: body_types.BucketOptions{Name: "avg_dummy_value_tree", Granularity: body_types.Granularity{Granularity: exportFrequency, Count: 3}},
			MergeFunction: body_types.RunnableExpression{Timeout: expressionTimeout, Expression: `
											aux = {"count":0, "value":0}
											for (i = 0; i < args.length; i++) {
												aux.count += args[i].count
												aux.value += args[i].value					
											}
											result = aux
											`},
			Levels:                               10,
			UpdateOnMembershipChange:             true,
			MaxFrequencyUpdateOnMembershipChange: 300 * time.Millisecond,
		})

	if err != nil {
		panic(err)
	}

	for range time.NewTicker(exportFrequency).C {
		res, err := cl.Query(
			"Select('avg_dummy_value_tree','*')",
			exportFrequency,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('avg_dummy_value_tree','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}

		if len(res) > 0 {
			valInt := res[0].Values[0].Fields["value"].(float64)
			writeOrPanic(csvWriter, []string{fmt.Sprintf("%d", int(valInt)), fmt.Sprintf("%d", time.Now().UnixNano())})
		}
	}
}

func benchmarkGlobalAggFunc(cl *client.DemmonClient) {
	csvWriter := setupCSVWriter(logFolder, "/results.csv", []string{"avg_dummy_value_tree", "timestamp"})
	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 3 * time.Second
		defaultTTL         = 2
		defaultMetricCount = 12
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 5 * time.Second
		requestTimeout     = 1 * time.Second
		queryBackoff       = 3 * time.Second
	)

	_, err := cl.InstallGlobalAggregationFunction(
		&body_types.GlobalAggregationFunction{
			MaxRetries: 3,
			Query: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `point = SelectLast("dummy_value","*")[0].Last()
							result = {"count":1, "value":point.Value().value}`,
			},
			OutputBucketOpts: body_types.BucketOptions{
				Name: "avg_dummy_value_tree",
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
		})

	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {
		res, err := cl.Query(
			"Select('avg_dummy_value_tree','*')",
			queryBackoff,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('avg_dummy_value_tree','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}

		if len(res) > 0 {
			valInt := res[0].Values[0].Fields["value"].(float64)
			writeOrPanic(csvWriter, []string{fmt.Sprintf("%d", int(valInt)), fmt.Sprintf("%d", time.Now().UnixNano())})
		}
	}
}
