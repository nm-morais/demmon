package engine

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
