module github.com/nm-morais/DeMMon

go 1.13

require (
	github.com/influxdata/influxdb v1.8.3
	github.com/influxdata/influxdb1-client v0.0.0-20200827194710-b269163b24ab
	github.com/nm-morais/deMMon-exporter v0.0.0-20201012104515-e38f0d5bdeb2
	github.com/nm-morais/go-babel v1.0.0

	// github.com/prometheus/tsdb v0.10.0
	github.com/sirupsen/logrus v1.7.0
	golang.org/x/tools v0.0.0-20201009162240-fcf82128ed91 // indirect
)

replace github.com/nm-morais/go-babel => ../go-babel

replace github.com/nm-morais/deMMon-exporter => ../deMMon-exporter
