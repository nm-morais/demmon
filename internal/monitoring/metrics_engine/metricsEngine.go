package metrics_engine

import (
	"math"
	"reflect"
	"regexp"
	"runtime/debug"

	// _ "github.com/robertkrimen/otto/underscore"

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

var errExpressionTimeout = errors.New("timeout running expression")

func (e *MetricsEngine) MakeQuery(expression string, timeoutDuration time.Duration) ([]tsdb.TimeSeries, error) {
	ottoVal, err := e.runWithTimeout(expression, timeoutDuration)
	if err != nil {
		return nil, err
	}
	vGeneric, err := ottoVal.Export()
	if err != nil {
		return nil, err
	}
	switch vConverted := vGeneric.(type) {
	case []tsdb.TimeSeries:
		return vConverted, nil
	case tsdb.TimeSeries:
		return []tsdb.TimeSeries{vConverted}, nil
	default:
		return nil, fmt.Errorf("unsupported return type %s", reflect.TypeOf(vGeneric))

	}
}

func (e *MetricsEngine) RunExpression(expression string, timeoutDuration time.Duration) error {
	_, err := e.runWithTimeout(expression, timeoutDuration)
	return err
}

func (e *MetricsEngine) runWithTimeout(expression string, timeoutDuration time.Duration) (*otto.Value, error) {
	start := time.Now()
	type returnType struct {
		ans *otto.Value
		err error
	}
	returnChan := make(chan returnType)
	defer close(returnChan)
	go func() {
		defer func() {
			duration := time.Since(start)
			if caught := recover(); caught != nil {
				fmt.Println("expression panicked: ", caught)
				if caught == errExpressionTimeout {
					returnChan <- returnType{
						ans: nil,
						err: errExpressionTimeout,
					}
					fmt.Fprintf(os.Stderr, "expression execution took longer then allowed (%v)", duration)
					return
				}
				returnChan <- returnType{
					ans: nil,
					err: fmt.Errorf("%+s", caught),
				}
				e.logger.Error(fmt.Sprintf("stacktrace from panic: %s", string(debug.Stack())))
			}
		}()
		vm := otto.New()
		e.setVmFunctions(vm)
		setDebuggerHandler(vm)
		vm.Interrupt = make(chan func(), 1)
		go func() {
			time.Sleep(timeoutDuration)
			vm.Interrupt <- func() {
				panic(errExpressionTimeout)
			}
		}()
		val, err := vm.Run(expression)
		returnChan <- returnType{
			ans: &val,
			err: err,
		}
	}()
	res := <-returnChan
	return res.ans, res.err
}

func (e *MetricsEngine) setVmFunctions(vm *otto.Otto) {
	err := vm.Set("Select", func(call otto.FunctionCall) otto.Value {
		return e.selectTs(vm, call)
	})
	if err != nil {
		panic(err)
	}

	err = vm.Set("SelectLast", func(call otto.FunctionCall) otto.Value {
		return e.selectLast(vm, call)
	})
	if err != nil {
		panic(err)
	}

	err = vm.Set("AddPoint", func(call otto.FunctionCall) otto.Value {
		return e.addPoint(vm, call)
	})
	if err != nil {
		panic(err)
	}
	err = vm.Set("Max", func(call otto.FunctionCall) otto.Value {
		return e.max(vm, call)
	})
	if err != nil {
		panic(err)
	}
	err = vm.Set("Min", func(call otto.FunctionCall) otto.Value {
		return e.min(vm, call)
	})
	if err != nil {
		panic(err)
	}
	err = vm.Set("Avg", func(call otto.FunctionCall) otto.Value {
		return e.avg(vm, call)
	})
	if err != nil {
		panic(err)
	}
}

func (e *MetricsEngine) selectTs(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	name, err := call.Argument(0).ToString()
	if err != nil {
		throw(vm, "Invalid arg: Name is not a string")
	}

	isTagFilterAll := call.Argument(1).IsString() && call.Argument(1).String() == "*"

	var queryResult []tsdb.TimeSeries
	if isTagFilterAll {
		b, ok := e.db.GetBucket(name)
		if !ok {
			throw(vm, fmt.Sprintf("No measurement found with name %s", name))
		}
		queryResult = b.GetAllTimeseries()
		res, err := vm.ToValue(queryResult)
		if err != nil {
			throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
		}
		return res
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
	queryResult = b.GetTimeseriesRegex(tags)
	res, err := vm.ToValue(queryResult)
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
	}
	return res
}

func (e *MetricsEngine) selectLast(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	name, err := call.Argument(0).ToString()
	if err != nil {
		throw(vm, "Invalid arg: Name is not a string")
	}

	isTagFilterAll := call.Argument(1).IsString() && call.Argument(1).String() == "*"

	var queryResult []tsdb.TimeSeries
	if isTagFilterAll {
		b, ok := e.db.GetBucket(name)
		if !ok {
			throw(vm, fmt.Sprintf("No measurement found with name %s", name))
		}
		queryResult = b.GetAllTimeseries()
		res, err := vm.ToValue(queryResult)
		if err != nil {
			throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
		}
		return res
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
	queryResult = b.GetTimeseriesRegexLastVal(tags)
	res, err := vm.ToValue(queryResult)
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
	}
	return res
}

func (e *MetricsEngine) addPoint(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	name, err := call.Argument(0).ToString()
	if err != nil {
		throw(vm, "AddPoint: Invalid arg: Name is not a string")
	}

	tagsObj := call.Argument(1).Object()
	if err != nil {
		throw(vm, "AddPoint: Invalid arg: tag filters is not defined")
	}

	tags := map[string]string{}
	tagKeys := tagsObj.Keys()
	for _, tagKey := range tagKeys {
		tagVal, err := tagsObj.Get(tagKey)
		if err != nil {
			throw(vm, "AddPoint: Invalid arg: tags is not a map[string]string")
		}
		tags[tagKey] = tagVal.String()
	}

	fieldsObj, err := call.Argument(2).Export()
	if err != nil {
		throw(vm, "AddPoint: Invalid arg (2): fields are not defined")
	}
	switch point := fieldsObj.(type) {
	case (map[string]interface{}):
		ts := e.db.GetOrCreateTimeseries(name, tags)
		pv := tsdb.PointValue{
			TS:     time.Now(),
			Fields: point,
		}
		ts.AddPoint(pv)
	case (*tsdb.PointValue):
		ts := e.db.GetOrCreateTimeseries(name, tags)
		pv := tsdb.PointValue{
			TS:     time.Now(),
			Fields: point.Fields,
		}
		ts.AddPoint(pv)
	default:
		throw(vm, fmt.Sprintf("AddPoint: Invalid arg (2): unsupported type %s", reflect.TypeOf(fieldsObj)))
	}
	return otto.Value{}
}

func (e *MetricsEngine) max(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 2 {
		throw(vm, fmt.Sprintf("Invalid args: not enough args, got: %d", len(call.ArgumentList)))
	}
	ts := call.Argument(0)
	ts.Object()
	tsArrGeneric, err := ts.Export()
	if err != nil {
		throw(vm, err.Error())
	}

	var fieldRegex *regexp.Regexp
	isTagFilterAll := call.Argument(1).IsString() && call.Argument(1).String() == "*"
	resultingName := ""
	fieldMaxs := map[string]interface{}{}

	if !isTagFilterAll {
		var err error
		fieldRegex, err = regexp.Compile(call.Argument(1).String())
		if err != nil {
			throw(vm, err.Error())
		}
	}
	switch toProcess := tsArrGeneric.(type) {
	case tsdb.TimeSeries:
		resultingName = toProcess.Name()
		max(vm, toProcess.All(), fieldRegex, fieldMaxs, isTagFilterAll)
	case []tsdb.TimeSeries:
		i := 0
		for _, ts := range toProcess {
			if i == 0 {
				resultingName = ts.Name()
			}
			if ts.Name() != resultingName {
				throw(vm, fmt.Sprintf("Cannot average timeseries from different measurements: %s != %s", ts.Name(), resultingName))
			}
			max(vm, ts.All(), fieldRegex, fieldMaxs, isTagFilterAll)
			i++
		}
	default:
		throw(vm, fmt.Sprintf("Unsupported input type %s", reflect.TypeOf(toProcess)))
	}

	toReturn := tsdb.NewTimeSeries(resultingName, make(map[string]string), tsdb.Granularity{Granularity: math.MaxInt64, Count: 2}, nil)
	toReturn.AddPoint(tsdb.PointValue{TS: time.Now(), Fields: fieldMaxs})
	res, err := vm.ToValue(toReturn.(tsdb.TimeSeries))
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming function output to js object: %s", err.Error()))
	}
	return res
}

func max(vm *otto.Otto, points []tsdb.PointValue, fieldRegex *regexp.Regexp, fieldMaxs map[string]interface{}, doAll bool) {
	for _, point := range points {
		for fieldKey, fieldVal := range point.Fields {
			if doAll || fieldRegex.MatchString(fieldKey) {
				fieldMax, ok := fieldMaxs[fmt.Sprintf("max_%s", fieldKey)]
				if !ok {
					fieldMax = -math.MaxFloat64
				}
				fieldValFloat, ok := fieldVal.(float64)
				if !ok {
					throw(vm, fmt.Sprintf("Function err: field %s is not of type float64 (%s)", fieldKey, reflect.TypeOf(fieldVal)))
				}
				fieldMaxs[fmt.Sprintf("max_%s", fieldKey)] = math.Max(fieldMax.(float64), fieldValFloat)
			}
		}
	}
}

func (e *MetricsEngine) min(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 2 {
		throw(vm, fmt.Sprintf("Invalid args: not enough args, got: %d", len(call.ArgumentList)))
	}
	ts := call.Argument(0)
	ts.Object()
	tsArrGeneric, err := ts.Export()
	if err != nil {
		throw(vm, err.Error())
	}

	isTagFilterAll := call.Argument(1).IsString() && call.Argument(1).String() == "*"

	resultingName := ""
	var fieldRegex *regexp.Regexp
	fieldMins := map[string]interface{}{}
	if !isTagFilterAll {
		var err error
		fieldRegex, err = regexp.Compile(call.Argument(1).String())
		if err != nil {
			throw(vm, err.Error())
		}
	}

	switch toProcess := tsArrGeneric.(type) {
	case tsdb.TimeSeries:
		resultingName = toProcess.Name()
		min(vm, toProcess.All(), fieldRegex, fieldMins, isTagFilterAll)
	case []tsdb.TimeSeries:
		i := 0
		for _, ts := range toProcess {
			if i == 0 {
				resultingName = ts.Name()
			}
			if ts.Name() != resultingName {
				throw(vm, fmt.Sprintf("Cannot average timeseries from different measurements:%s != %s", ts.Name(), resultingName))
			}
			min(vm, ts.All(), fieldRegex, fieldMins, isTagFilterAll)
			i++
		}
	default:
		throw(vm, fmt.Sprintf("Unsupported input type %s", reflect.TypeOf(toProcess)))
	}

	toReturn := tsdb.NewTimeSeries(resultingName, make(map[string]string), tsdb.Granularity{Granularity: math.MaxInt64, Count: 2}, nil)
	toReturn.AddPoint(tsdb.PointValue{TS: time.Now(), Fields: fieldMins})
	res, err := vm.ToValue(toReturn.(tsdb.TimeSeries))
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming function output to js object: %s", err.Error()))
	}
	return res
}

func min(vm *otto.Otto, points []tsdb.PointValue, fieldRegex *regexp.Regexp, fieldMaxs map[string]interface{}, doAll bool) {
	for _, point := range points {
		for fieldKey, fieldVal := range point.Fields {
			if doAll || fieldRegex.MatchString(fieldKey) {
				fieldMin, ok := fieldMaxs[fmt.Sprintf("min_%s", fieldKey)]
				if !ok {
					fieldMin = math.MaxFloat64
				}
				fieldValFloat, ok := fieldVal.(float64)
				if !ok {
					throw(vm, fmt.Sprintf("Function err: field %s is not of type float64 (%s)", fieldKey, reflect.TypeOf(fieldVal)))
				}
				fieldMaxs[fmt.Sprintf("min_%s", fieldKey)] = math.Min(fieldMin.(float64), fieldValFloat)
			}
		}
	}
}

type avgIntermediateCalc struct {
	Counter float64
	Value   float64
}

func (e *MetricsEngine) avg(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 2 {
		throw(vm, fmt.Sprintf("Invalid args: not enough args, got: %d", len(call.ArgumentList)))
	}
	ts := call.Argument(0)
	ts.Object()
	tsArrGeneric, err := ts.Export()
	if err != nil {
		throw(vm, err.Error())
	}
	var fieldRegex *regexp.Regexp
	isTagFilterAll := call.Argument(1).IsString() && call.Argument(1).String() == "*"
	resultingName := ""
	fieldCounters := make(map[string]*avgIntermediateCalc)
	if !isTagFilterAll {
		var err error
		fieldRegex, err = regexp.Compile(call.Argument(1).String())
		if err != nil {
			throw(vm, err.Error())
		}
	}

	switch toProcess := tsArrGeneric.(type) {
	case tsdb.TimeSeries:
		resultingName = toProcess.Name()
		avg(vm, toProcess.All(), fieldRegex, fieldCounters, isTagFilterAll)
	case []tsdb.TimeSeries:
		i := 0
		for _, ts := range toProcess {
			if i == 0 {
				resultingName = ts.Name()
			}
			if ts.Name() != resultingName {
				throw(vm, fmt.Sprintf("Cannot average timeseries from different measurements: %s != %s", ts.Name(), resultingName))
			}
			fieldCountersTmp := make(map[string]*avgIntermediateCalc)
			avg(vm, ts.All(), fieldRegex, fieldCountersTmp, isTagFilterAll)
			for fieldKey, fieldCounter := range fieldCountersTmp {
				fieldAvg := fieldCounter.Value / fieldCounter.Counter
				if _, ok := fieldCounters[fieldKey]; !ok {
					fieldCounters[fieldKey] = &avgIntermediateCalc{}
				}
				fieldCounters[fieldKey].Counter++
				fieldCounters[fieldKey].Value += fieldAvg
			}
			i++
		}
	default:
		throw(vm, fmt.Sprintf("Unsupported input type %s", reflect.TypeOf(toProcess)))
	}

	fieldAverages := make(map[string]interface{}, len(fieldCounters))
	for fieldKey, fieldCounter := range fieldCounters {
		fieldAverages[fieldKey] = fieldCounter.Value / fieldCounter.Counter
	}

	toReturn := tsdb.NewTimeSeries(resultingName, make(map[string]string), tsdb.Granularity{Granularity: math.MaxInt64, Count: 2}, nil)
	toReturn.AddPoint(tsdb.PointValue{TS: time.Now(), Fields: fieldAverages})
	res, err := vm.ToValue(toReturn.(tsdb.TimeSeries))
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming function output to js object: %s", err.Error()))
	}
	return res
}

func avg(vm *otto.Otto, points []tsdb.PointValue, fieldRegex *regexp.Regexp, fieldCounters map[string]*avgIntermediateCalc, doAll bool) {
	for _, point := range points {
		for fieldKey, fieldVal := range point.Fields {
			if doAll || fieldRegex.MatchString(fieldKey) {
				corresponfingFieldCounter, ok := fieldCounters[fmt.Sprintf("avg_%s", fieldKey)]
				if !ok {
					corresponfingFieldCounter = &avgIntermediateCalc{
						Counter: 0.0,
						Value:   0.0,
					}
				}
				fieldValFloat, ok := fieldVal.(float64)
				if !ok {
					throw(vm, fmt.Sprintf("Function err: field %s is not of type float64 (%s)", fieldKey, reflect.TypeOf(fieldVal)))
				}
				corresponfingFieldCounter.Counter++
				corresponfingFieldCounter.Value += fieldValFloat
				fieldCounters[fmt.Sprintf("avg_%s", fieldKey)] = corresponfingFieldCounter
			}
		}
	}
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
