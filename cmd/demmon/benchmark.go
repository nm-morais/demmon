package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"runtime"
	"time"

	client "github.com/nm-morais/demmon-client/pkg"
	"github.com/nm-morais/demmon-common/body_types"
	exporter "github.com/nm-morais/demmon-exporter"
)

type metricWithName struct {
	Name  string      `json:"name,omitempty"`
	Value interface{} `json:"values,omitempty"`
}

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

	go setupBenchmarkExporter(eConf, logFolder)
	if isLandmark {
		benchmarkTreeAggFunc(cl)
	}
}

func setupBenchmarkExporter(eConf *exporter.Conf, logFolder string) {
	csvWriter := setupCSVWriter(logFolder, "/local-values.csv", []string{"nr_goroutines", "timestamp"})

	e, err, errChan := exporter.New(eConf, GetLocalIP().String(), "demmon", nil)
	if err != nil {
		panic(err)
	}

	exportFrequncy := 5 * time.Second
	g := e.NewGauge("nr_goroutines", 12)
	setTicker := time.NewTicker(1 * time.Second)

	go func() {
		for {
			select {
			case <-setTicker.C:
				val := runtime.NumGoroutine()
				g.Set(float64(val))
				writeOrPanic(csvWriter, []string{fmt.Sprint(val), fmt.Sprintf("%d", time.Now().UnixNano())})
			case err := <-errChan:
				panic(err)
			}
		}
	}()
	e.ExportLoop(context.TODO(), exportFrequncy)
}

func writeOrPanic(csvWriter *csv.Writer, records []string) {
	if err := csvWriter.Write(records); err != nil {
		panic(err)
	}
	csvWriter.Flush()
}

func benchmarkTreeAggFunc(cl *client.DemmonClient) {
	csvWriter := setupCSVWriter(logFolder, "/results.csv", []string{"avg_nr_goroutines_tree", "timestamp"})
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

		if len(res) > 0 {
			valInt := res[0].Values[0].Fields["value"].(float64)
			writeOrPanic(csvWriter, []string{fmt.Sprintf("%d", int(valInt)), fmt.Sprintf("%d", time.Now().UnixNano())})
		}
	}
}
