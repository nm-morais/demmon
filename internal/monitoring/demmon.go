package monitoring

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/routes"
	"github.com/nm-morais/demmon/internal/membership/membership_frontend"
	"github.com/nm-morais/demmon/internal/monitoring/metrics_engine"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/reugn/go-quartz/quartz"
	"github.com/sirupsen/logrus"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type DemmonConf struct {
	Silent     bool
	LogFolder  string
	LogFile    string
	PluginDir  string
	ListenPort int
}

type continuousQueryValueType struct {
	mu           *sync.Mutex
	description  string
	d            *Demmon
	id           int
	queryTimeout time.Duration
	query        string
	err          error
	nrRetries    int
	triedNr      int
	lastRan      time.Time
}

// Description returns a PrintJob description.
func (cc continuousQueryValueType) Description() string {
	return cc.description
}

// Key returns a PrintJob unique key.
func (cc continuousQueryValueType) Key() int {
	return int(cc.id)
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (cc continuousQueryValueType) Execute() {
	fmt.Println("Executing " + cc.Description())
	cc.d.handleContinuousQueryTrigger(cc.id)
}

type client struct {
	id   uint64
	conn *websocket.Conn
	out  chan *body_types.Response
}

type Demmon struct {
	schedulerMu              *sync.Mutex
	scheduler                quartz.Scheduler
	continuousQueriesCounter *uint64
	logger                   *logrus.Logger
	counter                  *uint64
	conf                     DemmonConf
	db                       *tsdb.TSDB
	nodeUpdatesSubscribers   *sync.Map
	fm                       *membership_frontend.MembershipFrontend
	me                       *metrics_engine.MetricsEngine
}

func New(dConf DemmonConf, babel protocolManager.ProtocolManager) *Demmon {
	db := tsdb.GetDB()
	fm := membership_frontend.New(babel)
	d := &Demmon{
		schedulerMu:              &sync.Mutex{},
		scheduler:                quartz.NewStdScheduler(),
		continuousQueriesCounter: new(uint64),
		counter:                  new(uint64),
		logger:                   logrus.New(),
		nodeUpdatesSubscribers:   &sync.Map{},
		conf:                     dConf,
		db:                       db,
		fm:                       fm,
		me:                       metrics_engine.NewMetricsEngine(db),
	}
	d.scheduler.Start()
	setupLogger(d.logger, d.conf.LogFolder, d.conf.LogFile, d.conf.Silent)
	return d
}

func (d *Demmon) Listen() {
	r := mux.NewRouter()
	r.HandleFunc(routes.Dial, d.handleDial)
	err := http.ListenAndServe(fmt.Sprintf(":%d", d.conf.ListenPort), r)
	if err != nil {
		panic(err)
	}
}

func (d *Demmon) handleDial(w http.ResponseWriter, req *http.Request) {
	conn, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		d.logger.Println(err)
		return
	}
	client := &client{
		id:   atomic.AddUint64(d.counter, 1),
		conn: conn,
		out:  make(chan *body_types.Response),
	}
	go d.readPump(client)
	go d.writePump(client)
}

func (d *Demmon) handleRequest(r *body_types.Request, c *client) {

	d.logger.Info(r.Message)
	var ans interface{}
	var err error
	rType := routes.RequestType(r.Type)
	switch rType {
	case routes.GetInView:
		ans, err = d.getInView()
	case routes.MembershipUpdates:
		ans, err = d.subscribeNodeEvents(c)
	case routes.GetRegisteredMetricBuckets:
		ans = d.db.GetRegisteredBuckets()
	// case routes.RegisterMetrics:
	// 	reqBody := []*body_types.MetricMetadata{}
	// 	err = mapstructure.Decode(r.Message, &reqBody)
	// 	if err != nil {
	// 		err = errors.New("bad request body type")
	// 		break
	// 	}
	// 	for _, m := range reqBody {
	// 		d.logger.Infof("%+v", m)
	// 	}
	// 	err = d.mm.RegisterMetrics(reqBody)
	// 	if err != nil {
	// 		d.logger.Error(err)
	// 		break
	// 	}
	// 	ans = true
	case routes.PushMetricBlob:
		reqBody := body_types.PointCollectionWithTagsAndName{}
		err = decode(r.Message, &reqBody)
		if err != nil {
			d.logger.Error(err)
			err = errors.New("bad request body type")
			break
		}
		for _, m := range reqBody {
			d.db.AddMetric(m.Name, m.Tags, m.Point.Fields, m.Point.TS)
			// samples := d.db.GetOrCreateBucket(m.Name, tsdb.DefaultGranularity).GetOrCreateTimeseries(m.Tags).All()
			// d.logger.Info("Samples in timeseries:")
			// for _, s := range samples {
			// 	d.logger.Infof("%s : %+v", s.TS, s.Fields)
			// }
		}
		// err = d.db.AddMetric().AddMetricBlob(reqBody)
		if err != nil {
			d.logger.Error(err)
			break
		}
	case routes.Query:
		reqBody := body_types.QueryRequest{}
		err = decode(r.Message, &reqBody)
		if err != nil {
			d.logger.Error(err)
			err = errors.New("bad request body type")
			break
		}
		var queryResult []tsdb.TimeSeries
		queryResult, err = d.me.MakeQuery(reqBody.Query, reqBody.Timeout)
		if err != nil {
			break
		}
		toReturn := make([]body_types.Timeseries, len(queryResult))
		for idx, ts := range queryResult {
			tmpPts := ts.All()
			tmp := body_types.Timeseries{
				Name:   ts.Name(),
				Tags:   ts.Tags(),
				Points: make([]body_types.Point, len(tmpPts)),
			}
			for pointIdx, p := range tmpPts {
				tmp.Points[pointIdx] = body_types.Point{
					TS:     p.TS,
					Fields: p.Fields,
				}
			}
			toReturn[idx] = tmp
		}
		ans = toReturn
	case routes.InstallContinuousQuery:
		reqBody := body_types.InstallContinuousQueryRequest{}
		err = decode(r.Message, &reqBody)
		if err != nil {
			d.logger.Error(err)
			err = errors.New("bad request body type")
			break
		}
		aux := tsdb.Granularity{Granularity: time.Duration(reqBody.FrequencySeconds) * time.Second, Count: reqBody.OutputMetricCount}
		_, err = d.db.CreateBucket(reqBody.OutputMetricName, aux)
		if err != nil {
			break
		}
		taskId := atomic.AddUint64(d.continuousQueriesCounter, 1)
		trigger := quartz.NewSimpleTrigger(time.Duration(reqBody.FrequencySeconds) * time.Second)
		job := &continuousQueryValueType{
			mu:           &sync.Mutex{},
			description:  reqBody.Description,
			d:            d,
			err:          nil,
			id:           int(taskId),
			nrRetries:    reqBody.NrRetries,
			query:        reqBody.Expression,
			queryTimeout: reqBody.ExpressionTimeout,
			triedNr:      0,
		}
		d.schedulerMu.Lock()
		err = d.scheduler.ScheduleJob(job, trigger)
		if err != nil {
			d.schedulerMu.Unlock()
			break
		}
		ans = body_types.InstallContinuousQueryReply{
			TaskId: taskId,
		}
		d.schedulerMu.Unlock()
	case routes.GetContinuousQueries:
		resp := body_types.GetContinuousQueriesReply{}
		d.schedulerMu.Lock()
		jobKeys := d.scheduler.GetJobKeys()
		d.schedulerMu.Unlock()
		for _, jobKey := range jobKeys {
			d.schedulerMu.Lock()
			job, err := d.scheduler.GetScheduledJob(jobKey)
			d.schedulerMu.Unlock()
			if err != nil {
				continue
			}
			val := job.Job.(*continuousQueryValueType)
			val.mu.Lock()
			defer val.mu.Unlock()
			resp.ContinuousQueries = append(resp.ContinuousQueries, struct {
				TaskId    int
				NrRetries int
				CurrTry   int
				LastRan   time.Time
				Error     error
			}{
				TaskId:    val.id,
				Error:     val.err,
				LastRan:   val.lastRan,
				CurrTry:   val.triedNr,
				NrRetries: val.nrRetries,
			})
		}
		ans = resp
	case routes.IsMetricActive:
		panic("not yet implemented")
	case routes.BroadcastMessage:
		panic("not yet implemented")
	case routes.AlarmTrigger:
		panic("not yet implemented")
	default:
		err = errors.New("non-recognized operation")
	}

	d.logger.Infof("Got request %s, response: err: %+v, response:%+v", rType.String(), err, ans)
	c.out <- body_types.NewResponse(r.ID, false, err, r.Type, ans)
}

func (d *Demmon) subscribeNodeEvents(c *client) (body_types.NodeUpdateSubscriptionResponse, error) {
	d.nodeUpdatesSubscribers.Store(c.id, c)
	return body_types.NodeUpdateSubscriptionResponse{
		View: d.fm.GetInView(),
	}, nil
}

func (d *Demmon) getInView() (body_types.View, error) {
	return d.fm.GetInView(), nil
}

func (d *Demmon) readPump(c *client) {
	defer func() {
		c.conn.Close()
	}()
	for {
		req := &body_types.Request{}
		err := c.conn.ReadJSON(req)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseMessage, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				d.logger.Errorf("error: %v", err)
				break
			}
			d.logger.Errorf("error: %v", err)
			return
		}
		d.handleRequest(req, c)
	}
}

func (d *Demmon) writePump(c *client) {
	defer func() {
		c.conn.Close()
	}()
	for message := range c.out {
		err := c.conn.WriteJSON(message)
		if err != nil {
			d.logger.Errorf("error: %v", err)
			return
		}
	}
}

func (d *Demmon) handleContinuousQueryTrigger(taskId int) {
	d.schedulerMu.Lock()
	jobGeneric, err := d.scheduler.GetScheduledJob(taskId)
	if err != nil {
		d.schedulerMu.Unlock()
		d.logger.Error(err)
		return
	}
	d.schedulerMu.Unlock()
	job := jobGeneric.Job.(*continuousQueryValueType)
	d.logger.Infof("Continuous query %d trigger (%s)", taskId, job.query)
	err = d.me.RunExpression(job.query, job.queryTimeout)
	d.schedulerMu.Lock()
	defer d.schedulerMu.Unlock()
	if err != nil {
		d.logger.Errorf("Continuous query %d failed with error: %s", taskId, err)
		job.triedNr++
		job.err = err
		if job.triedNr == job.nrRetries {
			d.scheduler.DeleteJob(job.Key())
		}
		return
	}
	job.triedNr = 0
	job.lastRan = time.Now()
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
	logger.SetFormatter(&formatter{
		owner: "demmon_frontend",
		lf: &logrus.TextFormatter{
			DisableColors:   true,
			ForceColors:     false,
			FullTimestamp:   true,
			TimestampFormat: time.StampMilli,
		},
	})

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
		fmt.Println("Setting demmon_frontend non-silently")
	}
	logger.SetOutput(out)
}

func decode(input interface{}, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata: nil,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			toTimeHookFunc()),
		Result: result,
	})
	if err != nil {
		return err
	}

	if err := decoder.Decode(input); err != nil {
		return err
	}
	return err
}

func toTimeHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if t != reflect.TypeOf(time.Time{}) {
			return data, nil
		}

		switch f.Kind() {
		case reflect.String:
			return time.Parse(time.RFC3339, data.(string))
		case reflect.Float64:
			return time.Unix(0, int64(data.(float64))*int64(time.Millisecond)), nil
		case reflect.Int64:
			return time.Unix(0, data.(int64)*int64(time.Millisecond)), nil
		default:
			return data, nil
		}
		// Convert it by parsing
	}
}
