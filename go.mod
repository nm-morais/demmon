module github.com/nm-morais/demmon

go 1.15

require (
	github.com/benbjohnson/clock v1.0.3
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2
	github.com/mitchellh/mapstructure v1.3.3
	github.com/nm-morais/demmon-client v1.0.0
	github.com/nm-morais/demmon-common v1.0.0
	github.com/nm-morais/demmon-exporter v1.0.2
	github.com/nm-morais/go-babel v1.0.0
	github.com/reugn/go-quartz v0.3.2
	github.com/robertkrimen/otto v0.0.0-20200922221731-ef014fd054ac
	github.com/sirupsen/logrus v1.7.0
	gopkg.in/sourcemap.v1 v1.0.5 // indirect
	k8s.io/apimachinery v0.19.4
)

replace github.com/nm-morais/demmon-client => ../demmon-client

replace github.com/nm-morais/demmon-exporter => ../demmon-exporter

replace github.com/nm-morais/demmon-common => ../demmon-common

replace github.com/nm-morais/go-babel => ../go-babel
