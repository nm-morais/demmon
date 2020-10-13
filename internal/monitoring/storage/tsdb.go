package storage

import (
	"log"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type Metric struct {
	Name      string                 // e.g. "cpu_usage"
	Tags      map[string]string      // e.g. {"cpu": "cpu-total", "host": "host1", "region": "eu-west"}
	Fields    map[string]interface{} // e.g. {"idle": 0.1, "busy": 0.9}
	Timestamp time.Time
}

type TSDB struct {
	c client.Client
}

const (
	dbName    = "metrics"
	precision = "us"
)

func New() *TSDB {
	db := &TSDB{}
	// Create a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		log.Fatal(err)
	}
	db.c = c
	return db
}

func (db *TSDB) GetMetrics(query string) (res []client.Result, err error) {
	q := client.Query{
		Command:  query,
		Database: dbName,
	}
	if response, err := db.c.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

func (db *TSDB) AddMetrics(metrics []Metric) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  dbName,
		Precision: precision,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, metric := range metrics {
		pt, err := client.NewPoint(
			metric.Name,
			metric.Tags,
			metric.Fields,
			metric.Timestamp,
		)
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)
	}

	if err := db.c.Write(bp); err != nil {
		log.Fatal(err)
	}
}

func (db *TSDB) AddBatchPoints(bp client.BatchPoints) {
	bp.SetDatabase(dbName)
	if err := db.c.Write(bp); err != nil {
		log.Fatal(err)
	}
}
