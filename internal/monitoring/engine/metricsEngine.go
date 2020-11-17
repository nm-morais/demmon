package metrics_engine

import (
	_ "github.com/robertkrimen/otto/underscore"

	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/robertkrimen/otto"
	"github.com/sirupsen/logrus"
)

type MetricsEngine struct {
	logger *logrus.Logger
	db     *tsdb.TSDB
}

func NewMetricsEngine(db *tsdb.TSDB) *MetricsEngine {
	return &MetricsEngine{
		logger: logrus.New(),
		db:     db,
	}
}

var errTimeout = errors.New("timeout")

func (e *MetricsEngine) RunWithTimeout(expression string, timeoutDuration time.Duration) (otto.Value, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if caught := recover(); caught != nil {
			if caught == errTimeout {
				fmt.Fprintf(os.Stderr, "Some code took to long! Stopping after: %v\n", duration)
				return
			}
			panic(caught) // Something else happened, repanic!
		}
	}()

	vm := otto.New()
	e.setVmFunctions(vm)
	setDebuggerHandler(vm)
	vm.Interrupt = make(chan func(), 1) // The buffer prevents blocking
	go func() {
		time.Sleep(timeoutDuration) // Stop after two seconds
		vm.Interrupt <- func() {
			panic(errTimeout)
		}
	}()
	val, err := vm.Run(expression)
	return val, err
}

func (e *MetricsEngine) setVmFunctions(vm *otto.Otto) {
	vm.Set("select", func(call otto.FunctionCall) otto.Value {
		return e.selectTs(vm, call)
	})
	vm.Set("addPoint", func(call otto.FunctionCall) otto.Value {
		return e.addPoint(vm, call)
	})
	vm.Set("max", func(call otto.FunctionCall) otto.Value {
		return e.max(vm, call)
	})
}

func (e *MetricsEngine) selectTs(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	name, err := call.Argument(0).ToString()
	if err != nil {
		throw(vm, "Invalid arg: Name is not a string")
	}

	tagFilters := call.Argument(1).Object()
	if err != nil {
		throw(vm, "Invalid arg: tag filters is not defined")
	}

	tags := map[string]string{}
	tagKeys := tagFilters.Keys()
	for _, tagKey := range tagKeys {
		tagVal, err := tagFilters.Get(tagKey)
		if err != nil {
			throw(vm, "Invalid arg: tag filters is not a map[string]string")
		}
		tags[tagKey] = tagVal.String()
	}

	b, ok := e.db.GetBucket(name)
	if !ok {
		throw(vm, fmt.Sprintf("No measurement found with name %s", name))
	}

	queryResult := b.GetTimeseriesRegex(tags)
	res, err := vm.ToValue(queryResult)
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
	}
	return res
}

func (e *MetricsEngine) addPoint(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	name, err := call.Argument(0).ToString()
	if err != nil {
		throw(vm, "Invalid arg: Name is not a string")
	}

	tagsObj := call.Argument(1).Object()
	if err != nil {
		throw(vm, "Invalid arg: tag filters is not defined")
	}

	tags := map[string]string{}
	tagKeys := tagsObj.Keys()
	for _, tagKey := range tagKeys {
		tagVal, err := tagsObj.Get(tagKey)
		if err != nil {
			throw(vm, "Invalid arg: tags is not a map[string]string")
		}
		tags[tagKey] = tagVal.String()
	}

	fieldsObj := call.Argument(1).Object()
	if err != nil {
		throw(vm, "Invalid arg: fields are not defined")
	}
	fields := map[string]interface{}{}
	fieldKeys := fieldsObj.Keys()
	for _, fieldKey := range fieldKeys {
		fieldVal, err := fieldsObj.Get(fieldKey)
		if err != nil {
			throw(vm, "Invalid arg: fields is not a map[string]string")
		}
		fields[fieldKey] = fieldVal.String()
	}
	ts := e.db.GetOrCreateTimeseries(name, tags)
	pv := &tsdb.PointValue{
		TS:     time.Now(),
		Fields: fields,
	}
	ts.AddPoint(pv)
	return otto.Value{}
}

func (e *MetricsEngine) max(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	args := call.Argument(0)
	args.Object()
	tsArrGeneric, err := args.Export()
	if err != nil {
		throw(vm, err.Error())
	}
	for _, tsGeneric := range tsArrGeneric.([]tsdb.TimeSeries) {
		ts := tsGeneric.(tsdb.TimeSeries)
		fmt.Println(ts.Last())
		// throw(vm, tsArr)
	}

	return otto.Value{}
}

func setDebuggerHandler(vm *otto.Otto) {
	// This is where the magic happens!
	vm.SetDebuggerHandler(func(o *otto.Otto) {
		// The `Context` function is another hidden gem - I'll talk about that in
		// another post.
		c := o.Context()

		// Here, we go through all the symbols in scope, adding their names to a
		// list.
		var a []string
		for k := range c.Symbols {
			a = append(a, k)
		}

		sort.Strings(a)

		// Print out the symbols in scope.
		fmt.Printf("symbols in scope: %v\n", a)
	})
}

func throw(vm *otto.Otto, str string) {
	panic(vm.MakeCustomError("Runtime Error", str))
}

// var aggregationFunctions = map[string]govaluate.ExpressionFunction{
// 	"max": func(args ...interface{}) (interface{}, error) {
// 		var ts tsdb.TimeSeries
// 		var max float64 = -math.MaxFloat64
// 		switch converted := args[0].(type) {
// 		case tsdb.TimeSeries:
// 			ts = converted
// 		case string:
// 			aux, err := instance.db.GetTimeseries(args[0].(string))
// 			if err != nil {
// 				return nil, err
// 			}
// 			ts = aux
// 		case []float64:
// 			for _, s := range converted {
// 				max = math.Max(max, s)
// 			}
// 			return (float64)(max), nil
// 		}
// 		all := ts.All()
// 		fmt.Println("All:", all)
// 		for _, s := range all {
// 			max = math.Max(max, s.Value)
// 		}
// 		return (float64)(max), nil
// 	},
// 	"min": func(args ...interface{}) (interface{}, error) {
// 		var ts tsdb.TimeSeries
// 		var min float64 = math.MaxFloat64
// 		switch converted := args[0].(type) {
// 		case tsdb.TimeSeries:
// 			ts = converted
// 		case string:
// 			aux, err := instance.db.GetTimeseries(args[0].(string))
// 			if err != nil {
// 				return nil, err
// 			}
// 			ts = aux
// 		case []float64:
// 			for _, s := range converted {
// 				min = math.Min(min, s)
// 			}
// 			return (float64)(min), nil
// 		default:
// 			return nil, fmt.Errorf("Invalid type %s", reflect.TypeOf(args[0]))
// 		}
// 		all := ts.All()
// 		for _, s := range all {
// 			min = math.Min(min, s.Value)
// 		}
// 		return (float64)(min), nil

// 	},
// 	"avg": func(args ...interface{}) (interface{}, error) {
// 		var ts tsdb.TimeSeries
// 		var total float64
// 		switch converted := args[0].(type) {
// 		case tsdb.TimeSeries:
// 			ts = converted
// 		case string:
// 			aux, err := instance.db.GetTimeseries(args[0].(string))
// 			if err != nil {
// 				return nil, err
// 			}
// 			ts = aux
// 		case []float64:
// 			fmt.Println("Converted:", converted)
// 			for _, s := range converted {
// 				total += s
// 			}
// 			return (float64)(total / float64(len(converted))), nil
// 		default:
// 			return nil, fmt.Errorf("Invalid type %s", reflect.TypeOf(args[0]))
// 		}
// 		all := ts.All()
// 		fmt.Println("All:", all)
// 		for _, s := range all {
// 			total += s.Value
// 		}
// 		return (float64)(total / float64(len(all))), nil
// 	},
// 	"mode": func(args ...interface{}) (interface{}, error) {
// 		var ts tsdb.TimeSeries
// 		switch converted := args[0].(type) {
// 		case tsdb.TimeSeries:
// 			ts = converted
// 		case string:
// 			aux, err := instance.db.GetTimeseries(args[0].(string))
// 			if err != nil {
// 				return nil, err
// 			}
// 			ts = aux
// 		case []float64:
// 			fmt.Println("Converted:", converted)
// 			return getMode(converted), nil
// 		default:
// 			return nil, fmt.Errorf("Invalid type %s", reflect.TypeOf(args[0]))
// 		}
// 		all := ts.All()
// 		fmt.Println("All:", all)
// 		tmp := make([]float64, 0, len(all))
// 		for i, entry := range all {
// 			tmp[i] = entry.Value
// 		}
// 		return (float64)(getMode(tmp)), nil
// 	},
// 	"last": func(args ...interface{}) (interface{}, error) {
// 		var ts tsdb.TimeSeries
// 		switch converted := args[0].(type) {
// 		case tsdb.TimeSeries:
// 			ts = converted
// 		case string:
// 			aux, err := instance.db.GetTimeseries(args[0].(string))
// 			if err != nil {
// 				return nil, err
// 			}
// 			ts = aux
// 			if err != nil {
// 				return nil, err
// 			}
// 		default:
// 			return nil, fmt.Errorf("Invalid type %s", reflect.TypeOf(args[0]))
// 		}
// 		v, err := ts.Last()
// 		if err != nil {
// 			return nil, err
// 		}
// 		return (float64)(v.Value), nil
// 	},
// 	"range": func(args ...interface{}) (interface{}, error) {
// 		var ts tsdb.TimeSeries
// 		if len(args) != 3 {
// 			return nil, fmt.Errorf("Not enough argumments for range, need 3, have %d", len(args))
// 		}

// 		switch converted := args[0].(type) {
// 		case tsdb.TimeSeries:
// 			ts = converted
// 		case string:
// 			aux, err := instance.db.GetTimeseries(args[0].(string))
// 			if err != nil {
// 				return nil, err
// 			}
// 			ts = aux
// 			if err != nil {
// 				return nil, err
// 			}
// 		default:
// 			return nil, fmt.Errorf("Invalid type %s", reflect.TypeOf(args[0]))
// 		}

// 		start, ok := args[1].(float64)
// 		if !ok {
// 			return nil, fmt.Errorf("Invalid type for start time %s", reflect.TypeOf(args[1]))
// 		}

// 		end, ok := args[2].(float64)
// 		if !ok {
// 			return nil, fmt.Errorf("Invalid type for end time %s", reflect.TypeOf(args[2]))
// 		}

// 		startDate := time.Unix(0, int64(start))
// 		endDate := time.Unix(0, int64(end))

// 		points, err := ts.Range(startDate, endDate)
// 		if err != nil {
// 			return nil, err
// 		}
// 		values := make([]float64, len(points))
// 		for i, p := range points {
// 			values[i] = p.Value
// 		}
// 		return ([]float64)(values), nil
// 	},
// }

// var (
// 	instance *engine
// 	once     = &sync.Once{}
// )

// func New(tsbd *TSDB) *engine {
// 	once.Do(func() {
// 		instance = &engine{
// 			db: tsbd,
// 		}
// 	})
// 	return instance
// }

// type engine struct {
// 	db *TSDB
// }

// func (e *engine) evalNumericQuery(query string) (float64, error) {
// 	expression, err := govaluate.NewEvaluableExpressionWithFunctions(query, aggregationFunctions)
// 	if err != nil {
// 		return math.MaxFloat64, err
// 	}
// 	res, err := expression.Eval(nil)
// 	if err != nil {
// 		return math.MaxFloat64, err
// 	}

// 	val, ok := res.(float64)
// 	if !ok {
// 		return math.MaxFloat64, fmt.Errorf("query result type is not float64, is type %s", reflect.TypeOf(res))
// 	}
// 	return val, nil
// }

// func (e *engine) evalBoolQuery(query string) (bool, error) {
// 	expression, err := govaluate.NewEvaluableExpressionWithFunctions(query, aggregationFunctions)
// 	if err != nil {
// 		return false, err
// 	}
// 	res, err := expression.Eval(nil)
// 	if err != nil {
// 		return false, err
// 	}

// 	val, ok := res.(bool)
// 	if !ok {
// 		return false, fmt.Errorf("query result type is not bool, is type %s", reflect.TypeOf(res))
// 	}
// 	return val, nil
// }

// func (e *engine) getMetric(metricName string) (tsdb.TimeSeries, error) {
// 	return e.db.GetTimeseries(metricName)
// }

// func getMode(testArray []float64) (mode float64) {
// 	countMap := make(map[float64]int)
// 	for _, value := range testArray {
// 		countMap[value] += 1
// 	}
// 	max := 0
// 	for _, key := range testArray {
// 		freq := countMap[key]
// 		if freq > max {
// 			mode = key
// 			max = freq
// 		}
// 	}
// 	return
// }
