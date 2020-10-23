package tsdb

import "testing"

func TestAddMetrics(t *testing.T) {
	metrics := "nr_goroutines 21\n"
	db := NewTSDB()
	err := db.AddMetricBlob([]byte(metrics))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}
