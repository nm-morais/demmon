package storage

type TSDB struct {
}

func New() *TSDB {
	db := &TSDB{}
	return db
}

func (db *TSDB) GetMetrics(metricName string) (res, err error) {

	return res, nil
}

func (db *TSDB) AddMetrics() {

}
