module github.com/nm-morais/demmon

go 1.15

require (
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2
	github.com/mitchellh/mapstructure v1.3.3
	github.com/nm-morais/demmon-common v1.0.0
	github.com/nm-morais/demmon-exporter v1.0.2
	github.com/nm-morais/go-babel v1.0.0
	github.com/sirupsen/logrus v1.7.0
)

replace github.com/nm-morais/demmon-client => ../demmon-client

replace github.com/nm-morais/demmon-exporter => ../demmon-exporter

replace github.com/nm-morais/demmon-common => ../demmon-common

replace github.com/nm-morais/go-babel => ../go-babel
