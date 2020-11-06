package metrics_manager

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/timeseries"
	tsdb "github.com/nm-morais/demmon/internal/monitoring/TSDB"
	"github.com/nm-morais/demmon/internal/monitoring/plugin_manager"
)

var encoding base64.Encoding

type MetricsManager struct {
	registerLock                   *sync.Mutex
	registeredAggregationFunctions map[string]timeseries.AggregationFunc
	registeredMarshalFuncs         map[string]timeseries.MarshalFunc
	registeredUnmarshalFuncs       map[string]timeseries.UnmarshalFunc
	db                             *tsdb.TSDB
	pm                             *plugin_manager.PluginManager
}

func New(db *tsdb.TSDB, pm *plugin_manager.PluginManager) *MetricsManager {
	mm := &MetricsManager{
		registerLock:                   &sync.Mutex{},
		registeredAggregationFunctions: make(map[string]timeseries.AggregationFunc),
		registeredMarshalFuncs:         make(map[string]timeseries.MarshalFunc),
		registeredUnmarshalFuncs:       make(map[string]timeseries.UnmarshalFunc),
		pm:                             pm,
		db:                             db,
	}
	return mm
}

func (mm *MetricsManager) GetRegisteredMetrics() ([]string, error) {
	return mm.db.GetRegisteredTimeseries()
}

func (mm *MetricsManager) RegisterMetrics(newMetrics []*body_types.MetricMetadata) error {
	mm.registerLock.Lock()
	defer mm.registerLock.Unlock()
	for _, newMetric := range newMetrics {
		newMetricKey := formatMetricKey(newMetric.Service, newMetric.Name, newMetric.Sender)

		marshalFuncUnsafe, err := mm.pm.GetPluginSymbol(newMetric.Plugin, newMetric.MarshalFuncSymbolName)
		if err != nil {
			return err
		}
		marshalFunc, ok := marshalFuncUnsafe.(timeseries.MarshalFunc)
		if !ok {
			return errors.New("marshal function symbol is not of the correct type (timeseries.MarshalFunc)")
		}

		unmarshalFuncUnsafe, err := mm.pm.GetPluginSymbol(newMetric.Plugin, newMetric.UnmarshalFuncSymbolName)
		if err != nil {
			return err
		}
		unmarshalFunc, ok := unmarshalFuncUnsafe.(timeseries.UnmarshalFunc)
		if !ok {
			return errors.New("unmarshal function symbol is not of the correct type (timeseries.UnmarshalFunc)")
		}

		err = mm.db.AddTimeseries(newMetricKey, timeseries.WithGranularities(newMetric.Granularities...))
		if err != nil {
			return err
		}
		mm.registeredMarshalFuncs[fmt.Sprintf("%s/%s", newMetric.Service, newMetric.Name)] = marshalFunc
		mm.registeredUnmarshalFuncs[fmt.Sprintf("%s/%s", newMetric.Service, newMetric.Name)] = unmarshalFunc
	}
	return nil
}

func (mm *MetricsManager) AddMetricBlob(blob []string) error {
	for lineNr, line := range blob {
		lineSplit := strings.Split(line, " ")
		if len(lineSplit) != 3 {
			return fmt.Errorf("invalid metric in idx %d (%s)", lineNr, line)
		}
		metricKey := lineSplit[0]

		metricKeySplit := strings.Split(metricKey, "/")
		if len(metricKeySplit) != 3 {
			return fmt.Errorf("invalid metric identifier %s", metricKey)
		}
		service := metricKeySplit[0]
		metricName := metricKeySplit[1]

		metricValStr := lineSplit[1]
		tsStr := lineSplit[2]
		valueBytes, err := encoding.DecodeString(metricValStr)
		if err != nil {
			return fmt.Errorf("invalid base64 value in idx %d : %s", lineNr, metricValStr)
		}

		tsNum, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid timestamp value in idx %d : %s", lineNr, line)
		}
		ts := time.Unix(0, tsNum)

		metricMarshalFunc, ok := mm.registeredUnmarshalFuncs[fmt.Sprintf("%s/%s", service, metricName)]
		if !ok {
			return fmt.Errorf("no registered unmarshal func for metric %s", fmt.Sprintf("%s/%s", service, metricName))
		}

		v, err := metricMarshalFunc(valueBytes)
		if err != nil {
			return fmt.Errorf("unmarshal error: %s", metricKey)
		}

		addErr := mm.db.AddMetricAtTime(metricKey, v, ts)
		if addErr != nil {
			return addErr
		}
	}
	return nil
}

// func (mm *MetricsManager) deleteMetrics(w http.ResponseWriter, req *http.Request) {
// 	mm.logger.Info("Got request to delete metric blob")

// 	vars := mux.Vars(req)

// 	serviceName, ok := vars[routes.ServiceNamePathVar]
// 	if !ok {
// 		http.Error(w, "Missing service name", http.StatusBadRequest)
// 		return
// 	}

// 	metricName, ok := vars[routes.MetricNamePathVar]
// 	if !ok {
// 		http.Error(w, "Missing metric name", http.StatusBadRequest)
// 		return
// 	}

// 	origin, ok := vars[routes.OriginNamePathVar]
// 	if !ok {
// 		http.Error(w, "Missing origin in metric", http.StatusBadRequest)
// 		return
// 	}

// 	err := mm.db.DeleteTimeseries(formatMetricKey(serviceName, metricName, origin))
// 	if err != nil {
// 		mm.logger.Errorf("Got error %s deleting metrics", err)
// 		http.Error(w, err.Error(), err.Code())
// 		return
// 	}
// }

func formatMetricKey(service string, name string, origin string) string {
	return fmt.Sprintf("%s/%s/%s", service, name, origin)
}
