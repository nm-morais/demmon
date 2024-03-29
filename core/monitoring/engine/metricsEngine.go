package engine

import (
	"context"
	"io"
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

	pool "github.com/jolestar/go-commons-pool/v2"
	"github.com/nm-morais/demmon/core/monitoring/tsdb"
	"github.com/robertkrimen/otto"
	"github.com/sirupsen/logrus"
)

type Conf struct {
	Silent    bool
	LogFolder string
	LogFile   string
}
type MetricsEngine struct {
	logger *logrus.Logger
	db     *tsdb.TSDB
	pool   *pool.ObjectPool
}

func NewMetricsEngine(db *tsdb.TSDB, conf Conf, logToFile bool) *MetricsEngine {
	logger := logrus.New()
	me := &MetricsEngine{
		logger: logger,
		db:     db,
	}
	if logToFile {
		setupLogger(logger, conf.LogFolder, conf.LogFile, conf.Silent)
	}
	ctx := context.Background()
	p := pool.NewObjectPoolWithDefaultConfig(ctx, pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			vm := otto.New()
			me.setVMFunctions(vm)
			return vm, nil
		}))
	p.Config.MaxTotal = 10
	me.pool = p
	return me
}

var (
	errExpressionTimeout    = errors.New("timeout running expression")
	errUnsuportedReturnType = errors.New("unsupported return type")
)

func (e *MetricsEngine) borrowVM() *otto.Otto {
	obj1, err := e.pool.BorrowObject(context.TODO())
	if err != nil {
		panic(err)
	}

	return obj1.(*otto.Otto)
}
func (e *MetricsEngine) returnVM(obj *otto.Otto) {
	if err := e.pool.ReturnObject(context.TODO(), obj); err != nil {
		panic(err)
	}
}

func (e *MetricsEngine) MakeBoolQuery(expression string, timeoutDuration time.Duration) (bool, error) {
	vm := e.borrowVM()
	defer e.returnVM(vm)
	ottoVal, err := e.runWithTimeout(vm, expression, timeoutDuration)
	if err != nil {
		return false, err
	}

	vGeneric, err := ottoVal.Export()
	if err != nil {
		return false, err
	}

	e.logger.Infof("Query %s got result %+v", expression, vGeneric)

	switch vConverted := vGeneric.(type) {
	case bool:
		return vConverted, nil
	default:
		e.logger.Errorf("Unsupported return type: %s, (%+v)", reflect.TypeOf(vConverted), vConverted)
		return false, errUnsuportedReturnType
	}
}

func (e *MetricsEngine) MakeQuerySingleReturn(expression string, timeoutDuration time.Duration) (map[string]interface{}, error) {
	vm := e.borrowVM()
	defer e.returnVM(vm)
	ottoVal, err := e.runWithTimeout(vm, expression, timeoutDuration)

	if err != nil {
		e.logger.Errorf("QuerySingleReturn %s got error %s", expression, err.Error())
		return nil, err
	}
	vGeneric, err := ottoVal.Export()
	if err != nil {
		return nil, err
	}
	e.logger.Infof("QuerySingleReturn %s got result %+v", expression, vGeneric)

	switch vConverted := vGeneric.(type) {
	case map[string]interface{}:
		return vConverted, nil
	default:
		e.logger.Errorf("Unsupported return type: %s, (%+v)", reflect.TypeOf(vConverted), vConverted)
		return nil, errUnsuportedReturnType
	}
}

func (e *MetricsEngine) MakeQuery(expression string, timeoutDuration time.Duration) ([]tsdb.ReadOnlyTimeSeries, error) {
	vm := e.borrowVM()
	defer e.returnVM(vm)

	ottoVal, err := e.runWithTimeout(vm, expression, timeoutDuration)

	if err != nil {
		return nil, err
	}

	vGeneric, err := ottoVal.Export()
	if err != nil {
		return nil, err
	}

	e.logger.Infof("Query %s got result %+v", expression, vGeneric)

	switch vConverted := vGeneric.(type) {
	case []tsdb.ReadOnlyTimeSeries:
		return vConverted, nil
	case tsdb.ReadOnlyTimeSeries:
		return []tsdb.ReadOnlyTimeSeries{vConverted}, nil
	default:
		e.logger.Errorf("Unsupported return type: %s, (%+v)", reflect.TypeOf(vConverted), vConverted)
		return nil, errUnsuportedReturnType
	}
}

func (e *MetricsEngine) RunMergeFunc(expression string, timeoutDuration time.Duration, args []map[string]interface{}) (map[string]interface{}, error) {
	// make copy of args
	argsCopy := make([]map[string]interface{}, 0, len(args))
	for _, arg := range args {

		argCopy := map[string]interface{}{}
		for k, v := range arg {
			argCopy[k] = v
		}

		argsCopy = append(argsCopy, argCopy)
	}
	vm := e.borrowVM()
	defer e.returnVM(vm)
	err := vm.Set("args", argsCopy)
	if err != nil {
		_ = vm.Set("args", otto.UndefinedValue())
		return nil, err
	}
	ottoVal, err := e.runWithTimeout(vm, expression, timeoutDuration)
	_ = vm.Set("args", otto.UndefinedValue())
	if err != nil {
		return nil, err
	}

	vGeneric, err := ottoVal.Export()
	if err != nil {
		return nil, err
	}

	// e.logger.Infof("Merge function %s got result %+v", expression, vGeneric)
	switch vConverted := vGeneric.(type) {
	case map[string]interface{}:
		return vConverted, nil
	default:
		e.logger.Errorf("Unsupported return type: %s, (%+v)", reflect.TypeOf(vConverted), vConverted)
		return nil, errUnsuportedReturnType
	}
}

func (e *MetricsEngine) runWithTimeout(vm *otto.Otto, expression string, timeoutDuration time.Duration) (*otto.Value, error) {
	type returnType struct {
		ans *otto.Value
		err error
	}

	returnChan := make(chan returnType)
	done := make(chan interface{})

	go func() {
		defer func() {
			if caught := recover(); caught != nil {
				if caught == errExpressionTimeout.Error() {
					returnChan <- returnType{
						ans: nil,
						err: errExpressionTimeout,
					}
					return
				}

				e.logger.Errorf("got error: (%+v) running expression %s, stacktrace: \n %s", caught, expression, string(debug.Stack()))
				returnChan <- returnType{
					ans: nil,
					err: fmt.Errorf("%+v", caught),
				}
			}
		}()
		setDebuggerHandler(vm)
		vm.Interrupt = make(chan func(), 1)

		go func() {
			select {
			case <-time.After(timeoutDuration):
				vm.Interrupt <- func() {
					panic(errExpressionTimeout)
				}
			case <-done:
			}
		}()

		val, err := vm.Run(expression)

		if err != nil {
			returnChan <- returnType{
				ans: nil,
				err: err,
			}
			return
		}

		if val.IsUndefined() {
			var resVal otto.Value
			resVal, err = vm.Get("result")

			if err != nil {
				e.logger.Error(err)
			} else {
				val = resVal
			}
		}

		_ = vm.Set("result", otto.UndefinedValue())

		returnChan <- returnType{
			ans: &val,
			err: err,
		}
	}()
	res := <-returnChan
	close(done)
	close(returnChan)
	return res.ans, res.err
}

func (e *MetricsEngine) setVMFunctions(vm *otto.Otto) {
	err := vm.Set(
		"Select", func(call otto.FunctionCall) otto.Value {
			return e.selectTs(vm, &call)
		},
	)
	if err != nil {
		panic(err)
	}

	err = vm.Set(
		"SelectLast", func(call otto.FunctionCall) otto.Value {
			return e.selectLast(vm, &call)
		},
	)
	if err != nil {
		panic(err)
	}

	err = vm.Set(
		"SelectRange", func(call otto.FunctionCall) otto.Value {
			return e.selectRange(vm, &call)
		},
	)
	if err != nil {
		panic(err)
	}
	err = vm.Set(
		"Max", func(call otto.FunctionCall) otto.Value {
			return e.max(vm, &call)
		},
	)
	if err != nil {
		panic(err)
	}

	err = vm.Set(
		"Min", func(call otto.FunctionCall) otto.Value {
			return e.min(vm, call)
		},
	)
	if err != nil {
		panic(err)
	}

	err = vm.Set(
		"Avg", func(call otto.FunctionCall) otto.Value {
			return e.avg(vm, call)
		},
	)
	if err != nil {
		panic(err)
	}
}

func (e *MetricsEngine) selectTs(vm *otto.Otto, call *otto.FunctionCall) otto.Value {

	queryResult := make([]tsdb.ReadOnlyTimeSeries, 0)
	// defer e.logger.Infof("Select query result: %+v", queryResult)

	name, tagFilters, isTagFilterAll := extractSelectArgs(vm, call)

	if isTagFilterAll {
		b, ok := e.db.GetBucket(name)
		if ok {
			queryResult = b.GetAllTimeseries()
		}
		// for _, ts := range queryResult {
		// 	fmt.Printf("Select query result : %+v\n", ts)
		// }
		res, err := vm.ToValue(queryResult)
		if err != nil {
			throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
			return otto.Value{}
		}
		return res
	}

	b, ok := e.db.GetBucket(name)
	if ok {
		queryResult = b.GetTimeseriesRegex(tagFilters)
	}
	res, err := vm.ToValue(queryResult)
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
		return otto.Value{}
	}
	return res
}

func extractSelectArgs(vm *otto.Otto, call *otto.FunctionCall) (name string, tagFilters map[string]string, isTagFilterAll bool) {

	if len(call.ArgumentList) < 2 {
		throw(vm, "not enough args for select function")
	}

	name, err := call.Argument(0).ToString()
	if err != nil {
		throw(vm, "Invalid arg: Name is not a string")
	}
	isTagFilterAll = call.Argument(1).IsString() && call.Argument(1).String() == "*"
	if !isTagFilterAll {

		if call.Argument(1).IsUndefined() {
			throw(vm, "Invalid arg: tag filters is undefined")
		}

		tagFiltersGeneric := call.Argument(1).Object()
		tagFilters = map[string]string{}
		tagKeys := tagFiltersGeneric.Keys()
		for _, tagKey := range tagKeys {
			tagVal, err := tagFiltersGeneric.Get(tagKey)
			if err != nil {
				throw(vm, "Invalid arg: tag filters is not a map[string]string")
			}
			tagFilters[tagKey] = tagVal.String()
		}
	}
	return name, tagFilters, isTagFilterAll
}

func (e *MetricsEngine) selectLast(vm *otto.Otto, call *otto.FunctionCall) otto.Value {
	name, tagFilters, isTagFilterAll := extractSelectArgs(vm, call)
	var queryResult []tsdb.ReadOnlyTimeSeries
	if isTagFilterAll {
		b, ok := e.db.GetBucket(name)
		if ok {
			queryResult = b.GetAllTimeseriesLast()
		}
		res, err := vm.ToValue(queryResult)
		if err != nil {
			throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
			return otto.Value{}
		}
		return res
	}

	b, ok := e.db.GetBucket(name)
	if ok {
		queryResult = b.GetTimeseriesRegexLastVal(tagFilters)
	}

	res, err := vm.ToValue(queryResult)
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
		return otto.Value{}
	}
	return res
}

func (e *MetricsEngine) selectRange(vm *otto.Otto, call *otto.FunctionCall) otto.Value {
	name, tagFilters, isTagFilterAll := extractSelectArgs(vm, call)
	startTimeGeneric, err := call.Argument(2).ToInteger()
	if err != nil {
		throw(vm, fmt.Sprintf("err converting start date to integer: %s", err.Error()))
		return otto.Value{}
	}
	startTime := time.Unix(startTimeGeneric, 0)

	endTimeGeneric, err := call.Argument(3).ToInteger()
	if err != nil {
		throw(vm, fmt.Sprintf("err converting end date to integer: %s", err.Error()))
		return otto.Value{}
	}
	endTime := time.Unix(endTimeGeneric, 0)

	var queryResult []tsdb.ReadOnlyTimeSeries
	if isTagFilterAll {

		var b *tsdb.Bucket
		b, ok := e.db.GetBucket(name)
		if ok {
			queryResult = b.GetAllTimeseriesRange(startTime, endTime)
		}
		// e.logger.Infof("SelectRange query result: : %+v", queryResult)
		var res otto.Value
		res, err = vm.ToValue(queryResult)
		if err != nil {
			throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
			return otto.Value{}
		}
		return res
	}

	b, ok := e.db.GetBucket(name)
	if ok {
		queryResult = b.GetTimeseriesRangeRegex(tagFilters, startTime, endTime)
	}
	// e.logger.Infof("SelectRange query result: : %+v", queryResult)

	res, err := vm.ToValue(queryResult)
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
		return otto.Value{}
	}
	return res
}

func (e *MetricsEngine) max(vm *otto.Otto, call *otto.FunctionCall) otto.Value {

	if len(call.ArgumentList) != 2 {
		throw(vm, fmt.Sprintf("Invalid args: not enough args, got: %d", len(call.ArgumentList)))
		return otto.Value{}
	}
	ts := call.Argument(0)
	ts.Object()
	tsArrGeneric, err := ts.Export()
	if err != nil {
		throw(vm, err.Error())
		return otto.Value{}
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
			return otto.Value{}
		}
	}
	switch toProcess := tsArrGeneric.(type) {
	case tsdb.ReadOnlyTimeSeries:
		resultingName = toProcess.Name()
		max(vm, toProcess.All(), fieldRegex, fieldMaxs, isTagFilterAll)
	case []tsdb.ReadOnlyTimeSeries:
		i := 0
		for _, ts := range toProcess {
			if i == 0 {
				resultingName = ts.Name()
			}
			if ts.Name() != resultingName {
				throw(
					vm,
					fmt.Sprintf(
						"Cannot average timeseries from different measurements: %s != %s",
						ts.Name(),
						resultingName,
					),
				)
				return otto.Value{}
			}
			max(vm, ts.All(), fieldRegex, fieldMaxs, isTagFilterAll)
			i++
		}
	default:
		throw(vm, fmt.Sprintf("Unsupported input type %s", reflect.TypeOf(toProcess)))
		return otto.Value{}
	}

	if len(fieldMaxs) == 0 {
		throw(vm, "Max did not return any values")
		return otto.Value{}
	}

	toReturn := tsdb.NewStaticTimeSeries(
		resultingName,
		make(map[string]string),
		tsdb.NewObservable(fieldMaxs, time.Now()),
	)

	res, err := vm.ToValue(toReturn.(tsdb.ReadOnlyTimeSeries))
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming function output to js object: %s", err.Error()))
		return otto.Value{}
	}
	return res
}

func max(
	vm *otto.Otto,
	points []tsdb.Observable,
	fieldRegex *regexp.Regexp,
	fieldMaxs map[string]interface{},
	doAll bool,
) {
	for _, point := range points {
		for fieldKey, fieldVal := range point.Value() {
			if doAll || fieldRegex.MatchString(fieldKey) {
				fieldMax, ok := fieldMaxs[fmt.Sprintf("max_%s", fieldKey)]
				if !ok {
					fieldMax = -math.MaxFloat64
				}
				fieldValFloat, ok := fieldVal.(float64)
				if !ok {
					throw(
						vm,
						fmt.Sprintf(
							"Function err: field %s is not of type float64 (%s)",
							fieldKey,
							reflect.TypeOf(fieldVal),
						),
					)
					return
				}
				fieldMaxs[fmt.Sprintf("max_%s", fieldKey)] = math.Max(fieldMax.(float64), fieldValFloat)
			}
		}
	}
}

func (e *MetricsEngine) min(vm *otto.Otto, call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 2 {
		throw(vm, fmt.Sprintf("Invalid args: not enough args, got: %d", len(call.ArgumentList)))
		return otto.Value{}
	}
	ts := call.Argument(0)
	ts.Object()
	tsArrGeneric, err := ts.Export()
	if err != nil {
		throw(vm, err.Error())
		return otto.Value{}
	}

	isTagFilterAll := call.Argument(1).IsString() && call.Argument(1).String() == "*"

	resultingName := ""
	var fieldRegex *regexp.Regexp
	fieldMins := map[string]interface{}{}
	if !isTagFilterAll {
		fieldRegex, err = regexp.Compile(call.Argument(1).String())
		if err != nil {
			throw(vm, err.Error())
			return otto.Value{}
		}
	}

	switch toProcess := tsArrGeneric.(type) {
	case tsdb.ReadOnlyTimeSeries:
		resultingName = toProcess.Name()
		min(vm, toProcess.All(), fieldRegex, fieldMins, isTagFilterAll)
	case []tsdb.ReadOnlyTimeSeries:
		i := 0
		for _, ts := range toProcess {
			if i == 0 {
				resultingName = ts.Name()
			}
			if ts.Name() != resultingName {
				throw(
					vm,
					fmt.Sprintf(
						"Cannot average timeseries from different measurements:%s != %s",
						ts.Name(),
						resultingName,
					),
				)
				return otto.Value{}
			}
			min(vm, ts.All(), fieldRegex, fieldMins, isTagFilterAll)
			i++
		}
	default:
		throw(vm, fmt.Sprintf("Unsupported input type %s", reflect.TypeOf(toProcess)))
		return otto.Value{}
	}

	// toReturn := tsdb.NewTimeSeries(resultingName, make(map[string]string), tsdb.Granularity{Granularity: math.MaxInt64, Count: 2}, nil)
	// toReturn.AddPoint(tsdb.Observable{TS: time.Now(), Fields: fieldMins})

	if len(fieldMins) == 0 {
		throw(vm, "Min did not return any values")
		return otto.Value{}
	}

	toReturn := tsdb.NewStaticTimeSeries(
		resultingName,
		make(map[string]string),
		tsdb.NewObservable(fieldMins, time.Now()),
	)

	res, err := vm.ToValue(toReturn)
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming function output to js object: %s", err.Error()))
		return otto.Value{}
	}
	return res
}

func min(
	vm *otto.Otto,
	points []tsdb.Observable,
	fieldRegex *regexp.Regexp,
	fieldMins map[string]interface{},
	doAll bool,
) {
	for _, point := range points {
		for fieldKey, fieldVal := range point.Value() {
			if !doAll && fieldRegex.MatchString(fieldKey) {
				continue
			}

			fieldMin, ok := fieldMins[fmt.Sprintf("min_%s", fieldKey)]
			if !ok {
				fieldMin = math.MaxFloat64
			}

			fieldValFloat, ok := fieldVal.(float64)
			if !ok {
				throw(
					vm,
					fmt.Sprintf(
						"Function err: field %s is not of type float64 (%s)",
						fieldKey,
						reflect.TypeOf(fieldVal),
					),
				)
			}
			fieldMins[fmt.Sprintf("min_%s", fieldKey)] = math.Min(fieldMin.(float64), fieldValFloat)
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
		return otto.Value{}
	}
	ts := call.Argument(0)
	ts.Object()
	tsArrGeneric, err := ts.Export()
	if err != nil {
		throw(vm, err.Error())
		return otto.Value{}
	}

	var fieldRegex *regexp.Regexp
	isTagFilterAll := call.Argument(1).IsString() && call.Argument(1).String() == "*"
	resultingName := ""
	fieldCounters := make(map[string]*avgIntermediateCalc)
	if !isTagFilterAll {
		fieldRegex, err = regexp.Compile(call.Argument(1).String())
		if err != nil {
			throw(vm, err.Error())
			return otto.Value{}
		}
	}

	switch toProcess := tsArrGeneric.(type) {
	case tsdb.ReadOnlyTimeSeries:
		resultingName = toProcess.Name()
		avg(vm, toProcess.All(), fieldRegex, fieldCounters, isTagFilterAll)
	case []tsdb.ReadOnlyTimeSeries:
		i := 0
		for _, ts := range toProcess {
			if i == 0 {
				resultingName = ts.Name()
			}
			if ts.Name() != resultingName {
				throw(
					vm,
					fmt.Sprintf(
						"Cannot average timeseries from different measurements: %s != %s",
						ts.Name(),
						resultingName,
					),
				)
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
		return otto.Value{}
	}

	fieldAverages := make(map[string]interface{}, len(fieldCounters))
	for fieldKey, fieldCounter := range fieldCounters {
		fieldAverages[fieldKey] = fieldCounter.Value / fieldCounter.Counter
	}

	if len(fieldAverages) == 0 {
		throw(vm, "Average did not return any values")
		return otto.Value{}
	}

	// toReturn := tsdb.NewTimeSeries(resultingName, make(map[string]string), tsdb.Granularity{Granularity: math.MaxInt64, Count: 2}, nil)
	// toReturn.AddPoint(tsdb.Observable{TS: time.Now(), Fields: fieldAverages})
	toReturn := tsdb.NewStaticTimeSeries(
		resultingName,
		make(map[string]string),
		tsdb.NewObservable(fieldAverages, time.Now()),
	)
	res, err := vm.ToValue(toReturn)
	if err != nil {
		throw(vm, fmt.Sprintf("An error occurred transforming function output to js object: %s", err.Error()))
		return otto.Value{}
	}
	return res
}

func avg(
	vm *otto.Otto,
	points []tsdb.Observable,
	fieldRegex *regexp.Regexp,
	fieldCounters map[string]*avgIntermediateCalc,
	doAll bool,
) {
	for _, point := range points {
		for fieldKey, fieldVal := range point.Value() {

			if !(doAll || fieldRegex.MatchString(fieldKey)) {
				continue
			}

			corresponfingFieldCounter, ok := fieldCounters[fmt.Sprintf("avg_%s", fieldKey)]
			if !ok {
				corresponfingFieldCounter = &avgIntermediateCalc{
					Counter: 0.0,
					Value:   0.0,
				}
			}
			fieldValFloat, ok := fieldVal.(float64)
			if !ok {
				throw(
					vm,
					fmt.Sprintf(
						"Function err: field %s is not of type float64 (%s)",
						fieldKey,
						reflect.TypeOf(fieldVal),
					),
				)
			}
			corresponfingFieldCounter.Counter++
			corresponfingFieldCounter.Value += fieldValFloat
			fieldCounters[fmt.Sprintf("avg_%s", fieldKey)] = corresponfingFieldCounter
		}
	}
}

func (e *MetricsEngine) drop(vm *otto.Otto, call otto.FunctionCall) {
	name, err := call.Argument(0).ToString()
	if err != nil {
		throw(vm, "Invalid arg: Name is not a string")
	}
	isTagFilterAll := call.Argument(1).IsString() && call.Argument(1).String() == "*"
	if isTagFilterAll {
		b, ok := e.db.GetBucket(name)
		if !ok {
			throw(vm, fmt.Sprintf("No measurement found with name %s", name))
		}
		b.DropAll()
		return
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
			return
		}
		tags[tagKey] = tagVal.String()
	}

	b, ok := e.db.GetBucket(name)
	if !ok {
		throw(vm, fmt.Sprintf("No measurement found with name %s", name))
	}
	b.DropTimeseriesRegex(tags)
}

func setDebuggerHandler(vm *otto.Otto) {
	vm.SetDebuggerHandler(
		func(o *otto.Otto) {
			c := o.Context()
			var a []string
			for k := range c.Symbols {
				a = append(a, k)
			}
			sort.Strings(a)
			fmt.Printf("symbols in scope: %v\n", a)
		},
	)
}

func throw(vm *otto.Otto, str string) {
	panic(vm.MakeCustomError("Runtime Error", str))
}

type formatter struct {
	owner string
	lf    logrus.Formatter
}

func (f *formatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Message = fmt.Sprintf("[%s] %s", f.owner, e.Message)
	return f.lf.Format(e)
}

func setupLogger(logger *logrus.Logger, logFolder, logFile string, silent bool) {
	logger.SetFormatter(
		&formatter{
			owner: "metrics_engine",
			lf: &logrus.TextFormatter{
				DisableColors:   true,
				ForceColors:     false,
				FullTimestamp:   true,
				TimestampFormat: time.StampMilli,
			},
		},
	)

	if logFolder == "" {
		logger.Panicf("Invalid logFolder '%s'", logFolder)
	}
	if logFile == "" {
		logger.Panicf("Invalid logFile '%s'", logFile)
	}

	filePath := fmt.Sprintf("%s/%s", logFolder, logFile)
	err := os.MkdirAll(logFolder, 0777)
	if err != nil {
		logger.Panic(err)
	}
	file, err := os.Create(filePath)
	if os.IsExist(err) {
		var err = os.Remove(filePath)
		if err != nil {
			logger.Panic(err)
		}
		file, err = os.Create(filePath)
		if err != nil {
			logger.Panic(err)
		}
	}
	var out io.Writer = file
	if !silent {
		out = io.MultiWriter(os.Stdout, file)
	}
	logger.SetOutput(out)
}

// var aggregationFunctions = map[string]govaluate.ExpressionFunction{
// 	"max": func(args ...interface{}) (interface{}, error) {
// 		var ts tsdb.ReadOnlyTimeSeries
// 		var max float64 = -math.MaxFloat64
// 		switch converted := args[0].(type) {
// 		case tsdb.ReadOnlyTimeSeries:
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
// 		var ts tsdb.ReadOnlyTimeSeries
// 		var min float64 = math.MaxFloat64
// 		switch converted := args[0].(type) {
// 		case tsdb.ReadOnlyTimeSeries:
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
// 		var ts tsdb.ReadOnlyTimeSeries
// 		var total float64
// 		switch converted := args[0].(type) {
// 		case tsdb.ReadOnlyTimeSeries:
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
// 		var ts tsdb.ReadOnlyTimeSeries
// 		switch converted := args[0].(type) {
// 		case tsdb.ReadOnlyTimeSeries:
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
// 		var ts tsdb.ReadOnlyTimeSeries
// 		switch converted := args[0].(type) {
// 		case tsdb.ReadOnlyTimeSeries:
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
// 		var ts tsdb.ReadOnlyTimeSeries
// 		if len(args) != 3 {
// 			return nil, fmt.Errorf("Not enough argumments for range, need 3, have %d", len(args))
// 		}

// 		switch converted := args[0].(type) {
// 		case tsdb.ReadOnlyTimeSeries:
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

// func (e *engine) getMetric(metricName string) (tsdb.ReadOnlyTimeSeries, error) {
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

// func (e *MetricsEngine) newTs(vm *otto.Otto, call *otto.FunctionCall) otto.Value {
// 	name, err := call.Argument(0).ToString()
// 	if err != nil {
// 		throw(vm, "Invalid arg: Name is not a string")
// 	}

// 	tagsGeneric := call.Argument(1).Object()
// 	tagKeys := tagsGeneric.Keys()
// 	tags := map[string]string{}
// 	for _, tagKey := range tagKeys {
// 		tagVal, err := tagsGeneric.Get(tagKey)
// 		if err != nil {
// 			throw(vm, "Invalid arg: tag filters is not a map[string]string")
// 		}
// 		tags[tagKey] = tagVal.String()
// 	}

// 	valuesGeneric := call.Argument(1).Object()
// 	valuesKeys := valuesGeneric.Keys()
// 	values := map[string]interface{}{}
// 	for _, valKey := range valuesKeys {
// 		valVal, err := valuesGeneric.Get(valKey)
// 		if err != nil {
// 			throw(vm, "Invalid arg: values is not a map[string]Observable{}")
// 		}
// 		values[valKey] = tsdb.NewObservable(valVal.Object().Get(""))
// 	}

// 	res, err := vm.ToValue(tsdb.NewStaticTimeSeries(name, tags, values))
// 	if err != nil {
// 		throw(vm, fmt.Sprintf("An error occurred transforming timeseries to js object (%s)", err.Error()))
// 		return otto.Value{}
// 	}
// 	return res
// }

// func (e *MetricsEngine) addPoint(vm *otto.Otto, call otto.FunctionCall) otto.Value {
// 	name, err := call.Argument(0).ToString()
// 	if err != nil {
// 		throw(vm, "AddPoint: Invalid arg: Name is not a string")
// 	}

// 	tagsObj := call.Argument(1).Object()
// 	if err != nil {
// 		throw(vm, "AddPoint: Invalid arg: tag filters is not defined")
// 	}

// 	tags := map[string]string{}
// 	tagKeys := tagsObj.Keys()
// 	for _, tagKey := range tagKeys {
// 		tagVal, err := tagsObj.Get(tagKey)
// 		if err != nil {
// 			throw(vm, "AddPoint: Invalid arg: tags is not a map[string]string")
// 		}
// 		tags[tagKey] = tagVal.String()
// 	}

// 	fieldsObj, err := call.Argument(2).Export()
// 	if err != nil {
// 		throw(vm, "AddPoint: Invalid arg (2): fields are not defined")
// 	}
// 	switch point := fieldsObj.(type) {
// 	case (map[string]interface{}):
// 		ts, ok := e.db.GetTimeseries(name, tags)
// 		if !ok {
// 			throw(vm, "Destination bucket %s does not exist")
// 		}
// 		pv := tsdb.NewObservable(point, time.Now())
// 		ts.AddPoint(pv)
// 	case (tsdb.Observable):
// 		ts, ok := e.db.GetTimeseries(name, tags)
// 		if !ok {
// 			throw(vm, "Destination bucket does not exist")
// 		}
// 		pv := tsdb.NewObservable(point.Value(), point.TS())
// 		ts.AddPoint(pv)
// 	default:
// 		throw(vm, fmt.Sprintf("AddPoint: Invalid arg (2): unsupported type %s", reflect.TypeOf(fieldsObj)))
// 	}
// 	return otto.Value{}
// }
