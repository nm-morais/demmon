package engine

// var aggregationFunctions = map[string]govaluate.ExpressionFunction{
// 	"max": func(args ...interface{}) (interface{}, error) {
// 		var ts timeseries.TimeSeries
// 		var max float64 = -math.MaxFloat64
// 		switch converted := args[0].(type) {
// 		case timeseries.TimeSeries:
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
// 		var ts timeseries.TimeSeries
// 		var min float64 = math.MaxFloat64
// 		switch converted := args[0].(type) {
// 		case timeseries.TimeSeries:
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
// 		fmt.Println("All:", all)
// 		for _, s := range all {
// 			min = math.Min(min, s.Value)
// 		}
// 		return (float64)(min), nil

// 	},
// 	"avg": func(args ...interface{}) (interface{}, error) {
// 		var ts timeseries.TimeSeries
// 		var total float64
// 		switch converted := args[0].(type) {
// 		case timeseries.TimeSeries:
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
// 		var ts timeseries.TimeSeries
// 		switch converted := args[0].(type) {
// 		case timeseries.TimeSeries:
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
// 		var ts timeseries.TimeSeries
// 		switch converted := args[0].(type) {
// 		case timeseries.TimeSeries:
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
// 		var ts timeseries.TimeSeries
// 		if len(args) != 3 {
// 			return nil, fmt.Errorf("Not enough argumments for range, need 3, have %d", len(args))
// 		}

// 		switch converted := args[0].(type) {
// 		case timeseries.TimeSeries:
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
// 	once     sync.Once
// )

// func New(tsbd *tsdb.TSDB) *engine {
// 	once.Do(func() {
// 		instance = &engine{
// 			db: tsbd,
// 		}
// 	})
// 	return instance
// }

// type engine struct {
// 	db *tsdb.TSDB
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
