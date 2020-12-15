package monitoring

import (
	"errors"
	"fmt"
	"io"
	"math"
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
	membershipFrontend "github.com/nm-morais/demmon/internal/membership/frontend"
	"github.com/nm-morais/demmon/internal/monitoring/engine"
	monitoringProto "github.com/nm-morais/demmon/internal/monitoring/protocol"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/demmon/internal/utils"
	"github.com/reugn/go-quartz/quartz"
	"github.com/sirupsen/logrus"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var (
	ErrBadBodyType     = errors.New("bad request body type")
	ErrNonRecognizedOp = errors.New("non-recognized operation")
)

type DemmonConf struct {
	Silent     bool
	LogFolder  string
	LogFile    string
	ListenPort int
}

type continuousQueryValueType struct {
	mu               *sync.Mutex
	description      string
	id               int
	queryTimeout     time.Duration
	query            string
	err              error
	nrRetries        int
	triedNr          int
	lastRan          time.Time
	outputBucketOpts body_types.BucketOptions
}

type continuousQueryJobType struct {
	id int
	d  *Demmon
}

// Description returns a PrintJob description.
func (job continuousQueryJobType) Description() string {
	return ""
}

// Key returns a PrintJob unique key.
func (job continuousQueryJobType) Key() int {
	return job.id
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (job continuousQueryJobType) Execute() {
	job.d.handleContinuousQueryTrigger(job.id)
}

type client struct {
	id   uint64
	conn *websocket.Conn
	out  chan *body_types.Response
}

type Demmon struct {
	schedulerMu              *sync.Mutex
	scheduler                quartz.Scheduler
	continuousQueries        *sync.Map
	continuousQueriesCounter *uint64
	logger                   *logrus.Logger
	counter                  *uint64
	conf                     DemmonConf
	db                       *tsdb.TSDB
	nodeUpdatesSubscribers   *sync.Map
	monitorProto             *monitoringProto.Monitor
	fm                       *membershipFrontend.MembershipFrontend
	me                       *engine.MetricsEngine
}

func New(
	dConf DemmonConf,
	monitorProto *monitoringProto.Monitor,
	me *engine.MetricsEngine,
	db *tsdb.TSDB,
	fm *membershipFrontend.MembershipFrontend,
) *Demmon {
	d := &Demmon{
		continuousQueries:        &sync.Map{},
		schedulerMu:              &sync.Mutex{},
		scheduler:                quartz.NewStdScheduler(),
		continuousQueriesCounter: new(uint64),
		counter:                  new(uint64),
		logger:                   logrus.New(),
		nodeUpdatesSubscribers:   &sync.Map{},
		conf:                     dConf,
		monitorProto:             monitorProto,
		db:                       db,
		fm:                       fm,
		me:                       me,
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
		d.logger.Panic(err)
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
	d.logger.Infof("Got request %s with body; %+v", r.Type.String(), r.Message)

	var (
		ans interface{}
		err error
	)

	switch r.Type {
	case routes.GetInView:
		ans, err = d.getInView()
	case routes.MembershipUpdates:
		ans, err = d.subscribeNodeEvents(c)
	case routes.GetRegisteredMetricBuckets:
		ans = d.db.GetRegisteredBuckets()
	case routes.InstallBucket:
		reqBody := body_types.BucketOptions{}
		err = mapstructure.Decode(r.Message, &reqBody)
		if err != nil {
			break
		}
		// for _, m := range reqBody {
		_, err = d.db.CreateBucket(reqBody.Name, reqBody.Granularity.Granularity, reqBody.Granularity.Count)
		if err != nil {
			break
		}

		d.logger.Infof("Installed bucket: %+v", reqBody)
		// }
	case routes.PushMetricBlob:
		reqBody := body_types.PointCollectionWithTagsAndName{}
		err = decode(r.Message, &reqBody)

		if err != nil {
			d.logger.Error(err)
			err = ErrBadBodyType
			break
		}

		for _, m := range reqBody {
			d.logger.Info("Adding metric")
			err = d.db.AddMetric(m.Name, m.Tags, m.Point.Fields, m.Point.TS)

			if err != nil {
				d.logger.Errorf("Got error adding metric blob: %s", err.Error())
				break
			}

			d.logger.Info("Done adding metric...")
		}
	case routes.Query:
		reqBody := body_types.QueryRequest{}
		err = decode(r.Message, &reqBody)

		if err != nil {
			d.logger.Error(err)
			err = ErrBadBodyType
			break
		}

		var queryResult []tsdb.TimeSeries
		queryResult, err = d.me.MakeQuery(reqBody.Query.Expression, reqBody.Query.Timeout)

		if err != nil {
			break
		}

		toReturn := make([]body_types.Timeseries, 0, len(queryResult))

		for _, ts := range queryResult {
			allPts := ts.All()
			toReturnPts := make([]body_types.Point, 0, len(allPts))

			for _, p := range allPts {
				toReturnPts = append(
					toReturnPts, body_types.Point{
						TS:     p.TS(),
						Fields: p.Value(),
					},
				)
			}

			toReturn = append(
				toReturn, body_types.Timeseries{
					Name:   ts.Name(),
					Tags:   ts.Tags(),
					Points: toReturnPts,
				},
			)
		}

		ans = toReturn
	case routes.InstallContinuousQuery:
		reqBody := body_types.InstallContinuousQueryRequest{}
		err = decode(r.Message, &reqBody)

		if err != nil {
			d.logger.Error(err)
			err = ErrBadBodyType
			break
		}

		_, err = d.db.CreateBucket(
			reqBody.OutputBucketOpts.Name,
			reqBody.OutputBucketOpts.Granularity.Granularity,
			reqBody.OutputBucketOpts.Granularity.Count,
		)

		if err != nil {
			if err.Error() == "Bucket already exists" {
				break
			}

			panic(err)
		}

		d.logger.Infof("installed bucket: %+v", reqBody.OutputBucketOpts)

		taskId := atomic.AddUint64(d.continuousQueriesCounter, 1)
		trigger := quartz.NewSimpleTrigger(reqBody.OutputBucketOpts.Granularity.Granularity)
		cc := &continuousQueryValueType{
			mu:               &sync.Mutex{},
			description:      reqBody.Description,
			err:              nil,
			id:               int(taskId),
			nrRetries:        reqBody.NrRetries,
			query:            reqBody.Expression,
			queryTimeout:     reqBody.ExpressionTimeout,
			triedNr:          0,
			outputBucketOpts: reqBody.OutputBucketOpts,
		}
		d.continuousQueries.Store(cc.id, cc)
		d.schedulerMu.Lock()
		job := &continuousQueryJobType{
			id: int(taskId),
			d:  d,
		}
		err = d.scheduler.ScheduleJob(job, trigger)

		if err != nil {
			d.schedulerMu.Unlock()
			break
		}

		ans = body_types.InstallContinuousQueryReply{
			TaskId: taskId,
		}
		d.schedulerMu.Unlock()
		d.logger.Infof("installed continuous query: %+v", reqBody)

	case routes.GetContinuousQueries:
		resp := body_types.GetContinuousQueriesReply{}

		d.continuousQueries.Range(
			func(key, value interface{}) bool {
				job := value.(*continuousQueryValueType)
				job.mu.Lock()
				resp.ContinuousQueries = append(
					resp.ContinuousQueries, struct {
						TaskId    int
						NrRetries int
						CurrTry   int
						LastRan   time.Time
						Error     error
					}{
						TaskId:    job.id,
						Error:     job.err,
						LastRan:   job.lastRan,
						CurrTry:   job.triedNr,
						NrRetries: job.nrRetries,
					},
				)
				job.mu.Unlock()
				return true
			},
		)
		ans = resp
	case routes.InstallCustomInterestSet:
		d.logger.Panic("not yet implemented")
	case routes.InstallNeighborhoodInterestSet:
		reqBody := body_types.NeighborhoodInterestSet{}
		err = decode(r.Message, &reqBody)
		if err != nil {
			d.logger.Error(err)
			err = ErrBadBodyType
			break
		}

		d.logger.Infof("Creating bucket: %s", reqBody.OutputBucketOpts.Name)

		_, err = d.db.CreateBucket(
			reqBody.OutputBucketOpts.Name,
			reqBody.OutputBucketOpts.Granularity.Granularity,
			reqBody.OutputBucketOpts.Granularity.Count,
		)

		if err != nil {
			d.logger.Errorf("Got error installing neighborhood interest set: %s", err.Error())
			break
		}

		neighSetID := utils.GetRandInt(math.MaxInt64)
		d.monitorProto.AddNeighborhoodInterestSetReq(uint64(neighSetID), reqBody)
		d.logger.Infof("Added new neighborhood interest set: %s", reqBody.OutputBucketOpts.Name)
	case routes.BroadcastMessage:
		d.logger.Panic("not yet implemented")
	case routes.AlarmTrigger:
		d.logger.Panic("not yet implemented")
	default:
		err = ErrNonRecognizedOp
	}

	d.logger.Infof("Got request %s, response: err: %+v, response:%+v", r.Type.String(), err, ans)

	select {
	case c.out <- body_types.NewResponse(r.ID, false, err, r.Type, ans):
	case <-time.After(2 * time.Second):
		d.logger.Panic("TIMEOUT: Could not deliver response")
	}
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
		err := c.conn.Close()
		if err != nil {
			d.logger.Errorf("error closing connection: %w", err.Error())
		}
	}()
	for {
		req := &body_types.Request{}
		err := c.conn.ReadJSON(req)
		if err != nil {
			if websocket.IsUnexpectedCloseError(
				err,
				websocket.CloseMessage,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
			) {
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
		err := c.conn.Close()
		if err != nil {
			d.logger.Errorf("error closing the connection: %w", err)
		}
	}()

	for message := range c.out {
		err := c.conn.WriteJSON(message)
		if err != nil {
			d.logger.Errorf("error: %v", err)
			return
		}
	}
}

func (d *Demmon) handleContinuousQueryTrigger(taskID int) {
	jobGeneric, ok := d.continuousQueries.Load(taskID)
	if !ok {
		d.logger.Warn("Continuous query returning because it is not present in map")
		d.schedulerMu.Lock()
		defer d.schedulerMu.Unlock()
		err := d.scheduler.DeleteJob(taskID)

		if err != nil {
			d.logger.Errorf("Failed to delete continuous query: %s", err.Error())
		}

		return
	}

	job := jobGeneric.(*continuousQueryValueType)
	job.mu.Lock()
	defer job.mu.Unlock()
	d.logger.Infof("Continuous query %d trigger (%s)", taskID, job.description)
	res, err := d.me.MakeQuery(job.query, job.queryTimeout)
	if err != nil {
		d.logger.Errorf("Continuous query %d failed with error: %s", taskID, err)
		job.triedNr++
		job.err = err
		if job.triedNr == job.nrRetries {
			d.schedulerMu.Lock()
			defer d.schedulerMu.Unlock()
			err := d.scheduler.DeleteJob(job.id)
			if err != nil {
				d.logger.Errorf("Failed to delete continuous query: %s", err.Error())
			}
			return
		}
		return
	}

	for _, ts := range res {
		allPts := ts.All()
		if len(allPts) == 0 {
			d.logger.Error("Timeseries result is empty")
			continue
		}
		for _, pt := range allPts {
			err := d.db.AddMetric(job.outputBucketOpts.Name, ts.Tags(), pt.Value(), pt.TS())
			if err != nil {
				panic(err)
			}
		}
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
	logger.SetFormatter(
		&formatter{
			owner: "Demmon_Frontend",
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
		fmt.Println("Setting metrics_frontend non-silently")
	}
	logger.SetOutput(out)
}

func decode(input, result interface{}) error {
	decoder, err := mapstructure.NewDecoder(
		&mapstructure.DecoderConfig{
			Metadata: nil,
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				toTimeHookFunc(),
			),
			Result: result,
		},
	)
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
		data interface{},
	) (interface{}, error) {
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
