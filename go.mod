module github.com/nm-morais/demmon

go 1.15

require (
	github.com/nm-morais/demmon-exporter v1.0.2
	github.com/nm-morais/go-babel v1.0.0
	github.com/sirupsen/logrus v1.7.0
)

replace github.com/nm-morais/demmon-exporter => ../demmon-exporter
replace github.com/nm-morais/demmon-common => ../demmon-common
replace github.com/nm-morais/go-babel => ../go-babel
