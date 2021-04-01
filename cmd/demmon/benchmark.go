package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"strings"
	"time"

	client "github.com/nm-morais/demmon-client/pkg"
	"github.com/nm-morais/demmon-common/body_types"
	exporter "github.com/nm-morais/demmon-exporter"
)

type BenchmarkType = string

const (
	BenchmarkTreeAggFunc   = BenchmarkType("tree")
	BenchmarkGlobalAggFunc = BenchmarkType("global")
	BenchmarkNeighAggFunc  = BenchmarkType("neigh")
	BenchmarkAll           = BenchmarkType("all")
)

func benchmarkDemmonMetrics(eConf *exporter.Conf, isLandmark bool, benchmarkType BenchmarkType) {
	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 3 * time.Second
		defaultTTL         = 2
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
	time.Sleep(5 * time.Second)
	switch benchmarkType {
	case BenchmarkTreeAggFunc:
		benchmarkTreeAggFunc(cl, expressionTimeout, exportFrequency)
	case BenchmarkGlobalAggFunc:
		benchmarkGlobalAggFunc(cl, expressionTimeout, exportFrequency)
	case BenchmarkNeighAggFunc:
		benchmarkNeighAggFunc(cl, expressionTimeout, exportFrequency)
	case BenchmarkAll:
		go benchmarkTreeAggFunc(cl, expressionTimeout, exportFrequency)
		go benchmarkGlobalAggFunc(cl, expressionTimeout, exportFrequency)
		benchmarkNeighAggFunc(cl, expressionTimeout, exportFrequency)
	default:
		panic(fmt.Sprintf("unknown type of benchmark : <%s> specified for demmon", benchmarkType))
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
	csvWriter := setupCSVWriter(logFolder, "/results_tree.csv", []string{"avg_dummy_value_tree", "timestamp"})

	const (
		defaultMetricCount = 10
		maxRetries         = 3
	)

	cl.Lock()
	_, err := cl.InstallTreeAggregationFunction(
		&body_types.TreeAggregationSet{
			MaxRetries: maxRetries,
			Query: body_types.RunnableExpression{Timeout: expressionTimeout, Expression: `
															point = SelectLast("dummy_value","*")[0].Last()
															result = {"count":1, "value":point.Value().value}
															`},
			OutputBucketOpts: body_types.BucketOptions{Name: "avg_dummy_value_tree", Granularity: body_types.Granularity{Granularity: time.Duration(float32(exportFrequency) * 0.85), Count: defaultMetricCount}},
			MergeFunction: body_types.RunnableExpression{Timeout: expressionTimeout, Expression: `
															aux = {"count":0, "value":0}
															for (i = 0; i < args.length; i++) {
																aux.count += args[i].count
																aux.value += args[i].value					
															}
															result = aux
															`},
			Levels:                               -1,
			UpdateOnMembershipChange:             true,
			MaxFrequencyUpdateOnMembershipChange: 0 * time.Millisecond,
			StoreIntermediateValues:              true,
			IntermediateBucketOpts:               body_types.BucketOptions{Name: "avg_dummy_value_tree_child_values", Granularity: body_types.Granularity{Granularity: time.Duration(float32(exportFrequency) * 0.85), Count: defaultMetricCount}},
		})

	if err != nil {
		panic(err)
	}
	cl.Unlock()
	for range time.NewTicker(exportFrequency).C {
		cl.Lock()
		res, err := cl.Query(
			`SelectLast('avg_dummy_value_tree','*')`,
			exportFrequency,
		)
		cl.Unlock()

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

		// res, err = cl.Query(
		// 	"Select('avg_dummy_value_tree_child_values','*')",
		// 	exportFrequency,
		// )
		// if err != nil {
		// 	panic(err)
		// }

		// fmt.Println("Select('avg_dummy_value_tree_child_values','*') Query results :")

		// for idx, ts := range res {
		// 	fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		// }
	}
}

func benchmarkGlobalAggFunc(cl *client.DemmonClient, expressionTimeout, exportFrequency time.Duration) {
	csvWriter := setupCSVWriter(logFolder, "/results_global.csv", []string{"avg_dummy_value_global", "timestamp"})
	const (
		connectBackoffTime = 1 * time.Second
		defaultMetricCount = 10
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 3 * time.Second
		requestTimeout     = 1 * time.Second
		queryBackoff       = 3 * time.Second
	)

	cl.Lock()
	bucketName := "dummy_value_global"
	_, err := cl.InstallGlobalAggregationFunction(
		&body_types.GlobalAggregationFunction{
			Query: body_types.RunnableExpression{Timeout: expressionTimeout, Expression: `point = SelectLast("dummy_value","*")[0].Last()
											result = {"count":1, "value":point.Value().value}`},
			MergeFunction: body_types.RunnableExpression{Timeout: expressionTimeout, Expression: `
											aux = {"count":0, "value":0}
											for (i = 0; i < args.length; i++) {
												aux.count += args[i].count
												aux.value += args[i].value					
											}
											result = aux
											`},
			DifferenceFunction: body_types.RunnableExpression{Timeout: expressionTimeout, Expression: `
											toSubtractFrom = args[0]
											for (i = 1; i < args.length; i++) {
												toSubtractFrom.count -= args[i].count
												toSubtractFrom.value -= args[i].value					
											}
											result = toSubtractFrom
											`},
			OutputBucketOpts:        body_types.BucketOptions{Name: bucketName, Granularity: body_types.Granularity{Granularity: time.Duration(float32(exportFrequency) * 0.85), Count: defaultMetricCount}},
			MaxRetries:              3,
			StoreIntermediateValues: true,
			IntermediateBucketOpts:  body_types.BucketOptions{Name: "avg_dummy_value_global_peer_values", Granularity: body_types.Granularity{Granularity: time.Duration(float32(exportFrequency) * 0.85), Count: defaultMetricCount}},
		})
	cl.Unlock()
	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {
		cl.Lock()
		res, err := cl.Query(
			fmt.Sprintf("SelectLast('%s','*')", bucketName),
			queryBackoff,
		)
		cl.Unlock()
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('avg_dummy_value_global','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}

		if len(res) > 0 {
			valInt := res[0].Values[0].Fields["value"].(float64)
			writeOrPanic(csvWriter, []string{fmt.Sprintf("%d", int(valInt)), fmt.Sprintf("%d", time.Now().UnixNano())})
		}

		// res, err = cl.Query(
		// 	"Select('avg_dummy_value_global_peer_values','*')",
		// 	exportFrequency,
		// )
		// if err != nil {
		// 	panic(err)
		// }

		// fmt.Println("Select('avg_dummy_value_global_peer_values','*') Query results :")

		// for idx, ts := range res {
		// 	fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		// }
	}
}

func benchmarkNeighAggFunc(cl *client.DemmonClient, expressionTimeout, exportFrequency time.Duration) {
	csvWriter := setupCSVWriter(logFolder, "/results_neigh.csv", []string{"dummy_value_neigh", "hop", "timestamp"})
	const (
		connectBackoffTime = 1 * time.Second
		defaultMetricCount = 10
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 3 * time.Second
		requestTimeout     = 1 * time.Second
		queryBackoff       = 3 * time.Second
		TTL                = 2
	)

	cl.Lock()
	bucketName := "dummy_value_neigh"
	_, err := cl.InstallNeighborhoodInterestSet(
		&body_types.NeighborhoodInterestSet{
			StoreHopCountAsTag: true,
			TTL:                TTL,
			IS: body_types.InterestSet{
				MaxRetries: maxRetries,
				Query: body_types.RunnableExpression{
					Timeout:    expressionTimeout,
					Expression: `result = SelectLast("dummy_value","*")[0]`,
				},
				OutputBucketOpts: body_types.BucketOptions{
					Name: bucketName,
					Granularity: body_types.Granularity{
						Granularity: time.Duration(float32(exportFrequency) * 0.85),
						Count:       defaultMetricCount,
					},
				},
			},
		})
	cl.Unlock()
	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {
		for i := 1; i <= TTL; i++ {

			startTime := time.Now().Add(-time.Duration(float32(exportFrequency) * 1.5))
			endTime := time.Now()
			cl.Lock()
			res, err := cl.Query(
				fmt.Sprintf("SelectRange('%s',{'hop_nr': %d}, %d, %d )", bucketName, i, startTime.Unix(), endTime.Unix()),
				queryBackoff,
			)
			cl.Unlock()

			if err != nil {
				if strings.Contains(err.Error(), "did not return any results") {
					continue
				}
				panic(err)
			}

			fmt.Println()
			fmt.Println("Query results :")

			for idx, ts := range res {
				// valInt := ts.Values[0].Fields["value"].(float64)
				fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
			}

			writeOrPanic(csvWriter, []string{fmt.Sprintf("%d", len(res)), fmt.Sprintf("%d", i), fmt.Sprintf("%d", time.Now().UnixNano())})
		}
		// res, err = cl.Query(
		// 	"Select('avg_dummy_value_neigh_peer_values','*')",
		// 	exportFrequency,
		// )
		// if err != nil {
		// 	panic(err)
		// }

		// fmt.Println("Select('avg_dummy_value_neigh_peer_values','*') Query results :")

		// for idx, ts := range res {
		// 	fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		// }
	}
}
