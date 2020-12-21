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
	"github.com/mitchellh/hashstructure/v2"
	"github.com/mitchellh/mapstructure"
	demmon_client "github.com/nm-morais/demmon-client/pkg"
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/routes"
	membershipFrontend "github.com/nm-morais/demmon/internal/membership/frontend"
	"github.com/nm-morais/demmon/internal/monitoring/engine"
	monitoringProto "github.com/nm-morais/demmon/internal/monitoring/protocol"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
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

const (
	DeliverRequestResponseTimeout = 2 * time.Second
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

type customInterestSetWrapper struct {
	nrRetries int
	is        body_types.CustomInterestSet
	*sync.Mutex
}

type customInterestSetValueType = *customInterestSetWrapper

type continuousQueryJobWrapper struct {
	id int
	d  *Demmon
}

// Description returns a PrintJob description.
func (job continuousQueryJobWrapper) Description() string {
	return ""
}

// Key returns a PrintJob unique key.
func (job continuousQueryJobWrapper) Key() int {
	return job.id
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (job continuousQueryJobWrapper) Execute() {
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
	customInterestSets       *sync.Map
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
		customInterestSets:       &sync.Map{},
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

	var resp *body_types.Response

	switch r.Type {
	case routes.GetInView:
		ans, err := d.getInView()
		if err != nil {
			d.logger.Errorf("Got error fetching view: %s", err.Error())
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)

			break
		}
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, ans)
	case routes.MembershipUpdates:
		ans, err := d.subscribeNodeEvents(c)
		if err != nil {
			d.logger.Errorf("Got error subscribing to node events: %s", err.Error())
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, ans)

			break
		}
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, ans)
	case routes.GetRegisteredMetricBuckets:
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, d.db.GetRegisteredBuckets())
	case routes.InstallBucket:
		reqBody := body_types.BucketOptions{}
		err := mapstructure.Decode(r.Message, &reqBody)
		if err != nil {
			resp = body_types.NewResponse(r.ID, false, err, 400, r.Type, nil)
			break
		}
		// for _, m := range reqBody {
		_, err = d.db.CreateBucket(reqBody.Name, reqBody.Granularity.Granularity, reqBody.Granularity.Count)
		if err != nil {
			if errors.Is(err, tsdb.ErrAlreadyExists) {
				resp = body_types.NewResponse(r.ID, false, err, 409, r.Type, nil)
				break
			}
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
			break
		}

		d.logger.Infof("Installed bucket: %+v", reqBody)
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, nil)
		// }
	case routes.PushMetricBlob:
		reqBody := []*body_types.TimeseriesDTO{}

		d.logger.Infof("Adding: %+v, type:%s", r.Message, reflect.TypeOf(r.Message))

		err := decode(r.Message, &reqBody)
		if err != nil {
			d.logger.Error(err)
			err = ErrBadBodyType
			resp = body_types.NewResponse(r.ID, false, err, 400, r.Type, nil)
			break
		}
		d.logger.Infof("Decoded: %+v, type:%s", reqBody, reflect.TypeOf(reqBody))
		tsArr := make([]tsdb.ReadOnlyTimeSeries, 0, len(reqBody))
		for _, ts := range reqBody {
			tsArr = append(tsArr, tsdb.StaticTimeseriesFromDTO(ts))
		}
		err = d.db.AddAll(tsArr)
		if err != nil {
			d.logger.Errorf("Got error adding metric blob: %s", err.Error())
			if errors.Is(err, tsdb.ErrBucketNotFound) {
				resp = body_types.NewResponse(r.ID, false, err, 404, r.Type, nil)
				break
			}
		}

		// for _, m := range reqBody {
		// 	d.logger.Info("Adding metric")
		// 	err = d.db.AddMetric(m.Name, m.Tags, m.Point.Fields, m.Point.TS)
		// 	if err != nil {
		// 		d.logger.Errorf("Got error adding metric blob: %s", err.Error())
		// 		if errors.Is(err, tsdb.ErrBucketNotFound) {
		// 			resp = body_types.NewResponse(r.ID, false, err, 409, r.Type, nil)
		// 			break switchLabel
		// 		}
		// 		resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
		// 		break switchLabel
		// 	}

		// 	d.logger.Info("Done adding metric...")
		// }

		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, nil)
	case routes.Query:
		reqBody := body_types.QueryRequest{}
		err := decode(r.Message, &reqBody)

		if err != nil {
			d.logger.Error(err)
			err = ErrBadBodyType
			resp = body_types.NewResponse(r.ID, false, err, 400, r.Type, nil)
			break
		}

		var queryResult []tsdb.ReadOnlyTimeSeries
		queryResult, err = d.me.MakeQuery(reqBody.Query.Expression, reqBody.Query.Timeout)
		if err != nil {
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
			break
		}

		// toReturn := make([]body_types.ReadOnlyTimeSeries, 0, len(queryResult))

		// for _, ts := range queryResult {
		// 	allPts := ts.All()
		// 	toReturnPts := make([]body_types.Point, 0, len(allPts))
		// 	for _, p := range allPts {
		// 		toReturnPts = append(
		// 			toReturnPts, body_types.Point{
		// 				TS:     p.TS(),
		// 				Fields: p.Value(),
		// 			},
		// 		)
		// 	}
		// 	toReturn = append(toReturn, body_types.NewStaticTimeSeries(ts.Name(), ts.Tags(), toReturnPts))
		// }

		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, queryResult)
	case routes.InstallContinuousQuery:
		reqBody := body_types.InstallContinuousQueryRequest{}
		err := decode(r.Message, &reqBody)

		if err != nil {
			d.logger.Error(err)
			err = ErrBadBodyType
			resp = body_types.NewResponse(r.ID, false, err, 400, r.Type, nil)
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
			resp = body_types.NewResponse(r.ID, false, err, 409, r.Type, nil)
			break
		}

		d.logger.Infof("installed bucket: %+v", reqBody.OutputBucketOpts)

		taskID := atomic.AddUint64(d.continuousQueriesCounter, 1)
		trigger := quartz.NewSimpleTrigger(reqBody.OutputBucketOpts.Granularity.Granularity)
		cc := &continuousQueryValueType{
			mu:               &sync.Mutex{},
			description:      reqBody.Description,
			err:              nil,
			id:               int(taskID),
			nrRetries:        reqBody.NrRetries,
			query:            reqBody.Expression,
			queryTimeout:     reqBody.ExpressionTimeout,
			triedNr:          0,
			outputBucketOpts: reqBody.OutputBucketOpts,
		}
		d.continuousQueries.Store(cc.id, cc)
		d.schedulerMu.Lock()
		job := &continuousQueryJobWrapper{
			id: int(taskID),
			d:  d,
		}
		err = d.scheduler.ScheduleJob(job, trigger)

		if err != nil {
			d.schedulerMu.Unlock()
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
			break
		}

		ans := body_types.InstallContinuousQueryReply{
			TaskID: taskID,
		}
		d.schedulerMu.Unlock()
		d.logger.Infof("installed continuous query: %+v", reqBody)
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, ans)
	case routes.GetContinuousQueries:
		ans := body_types.GetContinuousQueriesReply{}
		d.continuousQueries.Range(
			func(key, value interface{}) bool {
				job := value.(*continuousQueryValueType)
				job.mu.Lock()
				ans.ContinuousQueries = append(
					ans.ContinuousQueries, struct {
						TaskID    int
						NrRetries int
						CurrTry   int
						LastRan   time.Time
						Error     error
					}{
						TaskID:    job.id,
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
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, ans)
	case routes.InstallCustomInterestSet:
		reqBody := body_types.CustomInterestSet{}
		err := decode(r.Message, &reqBody)
		if err != nil {
			d.logger.Error(err)
			err = ErrBadBodyType
			resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, nil)
			break
		}

		d.logger.Infof("Creating custom interest set func output bucket: %s", reqBody.IS.OutputBucketOpts.Name)

		_, err = d.db.CreateBucket(
			reqBody.IS.OutputBucketOpts.Name,
			reqBody.IS.OutputBucketOpts.Granularity.Granularity,
			reqBody.IS.OutputBucketOpts.Granularity.Count,
		)

		if err != nil {
			d.logger.Errorf("Got error installing custom interest set: %s", err.Error())
			if errors.Is(err, tsdb.ErrAlreadyExists) {
				resp = body_types.NewResponse(r.ID, false, err, 409, r.Type, nil)
				break
			}
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
			break
		}

		// d.logger.Panic("not yet implemented")
	case routes.InstallNeighborhoodInterestSet:
		reqBody := body_types.NeighborhoodInterestSet{}
		err := decode(r.Message, &reqBody)
		if err != nil {
			d.logger.Error(err)
			err = ErrBadBodyType
			resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, nil)
			break
		}

		d.logger.Infof("Creating neigh interest set func output bucket: %s", reqBody.IS.OutputBucketOpts.Name)

		_, err = d.db.CreateBucket(
			reqBody.IS.OutputBucketOpts.Name,
			reqBody.IS.OutputBucketOpts.Granularity.Granularity,
			reqBody.IS.OutputBucketOpts.Granularity.Count,
		)

		if err != nil {
			d.logger.Errorf("Got error installing neighborhood interest set: %s", err.Error())
			if errors.Is(err, tsdb.ErrAlreadyExists) {
				resp = body_types.NewResponse(r.ID, false, err, 409, r.Type, nil)
				break
			}
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
			break
		}
		neighSetID := Hash(reqBody.IS)
		d.monitorProto.AddNeighborhoodInterestSetReq(neighSetID, reqBody)
		d.logger.Infof("Added new neighborhood interest set: %+v", reqBody)
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, neighSetID)

	case routes.BroadcastMessage:
		d.logger.Panic("not yet implemented")
	case routes.AlarmTrigger:
		d.logger.Panic("not yet implemented")
	default:
		resp = body_types.NewResponse(r.ID, false, ErrNonRecognizedOp, 400, r.Type, nil)
	}

	if resp.Error {
		d.logger.Errorf("Got request %s, response: status: %d, err: %s", r.Type.String(), resp.Code, resp.GetMsgAsErr().Error())
	} else {
		d.logger.Infof("Got request %s, response: status:% d, response: %+v", r.Type.String(), resp.Code, resp.Message)
	}

	select {
	case c.out <- resp:
	case <-time.After(DeliverRequestResponseTimeout):
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
			d.logger.Errorf("error closing connection: %s", err.Error())
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

func (d *Demmon) handleCustomInterestSet(taskID int) { // TODO FIXME
	jobGeneric, ok := d.customInterestSets.Load(taskID)
	if !ok {
		return
	}
	job := jobGeneric.(*body_types.CustomInterestSet)
	clients := make([]*demmon_client.DemmonClient, len(job.Hosts))
	for _, p := range job.Hosts { // TODO set ports on hosts instead
		newCL := demmon_client.New(demmon_client.DemmonClientConf{
			DemmonPort:     d.conf.ListenPort, // TODO remove this
			DemmonHostAddr: p.IP.String(),
			RequestTimeout: job.IS.Query.Timeout,
		})

		i := 0
		for ; ; i++ {
			err := newCL.ConnectTimeout(job.IS.Query.Timeout)
			if err != nil {
				if i == job.IS.MaxRetries {
					// TODO give error
					return
				}
				continue
			}
			break
		}
	}

	ticker := time.NewTicker(job.IS.OutputBucketOpts.Granularity.Granularity)
	for range ticker.C {
		jobGeneric, ok = d.customInterestSets.Load(taskID)
		if !ok {
			d.logger.Info("Custom interest set %d returning", taskID)
			return
		}
		job := jobGeneric.(customInterestSetValueType)
		query := job.is.IS.Query
		for _, cl := range clients {
			res, err := cl.Query(query.Expression, query.Timeout)
			if err != nil {
				job.Lock()
				job.nrRetries++
				job.Unlock()
			}
			toAdd := []tsdb.ReadOnlyTimeSeries{}
			for _, ts := range res {
				toAdd = append(toAdd, tsdb.StaticTimeseriesFromDTO(ts))
			}
			d.db.AddAll(toAdd) // TODO
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
		ts.(tsdb.TimeSeries).SetName(job.outputBucketOpts.Name)

		// allPts := ts.All()
		// if len(allPts) == 0 {
		// 	d.logger.Error("Timeseries result is empty")
		// 	continue
		// }
		// for _, pt := range allPts {
		// 	err := d.db.AddMetric(job.outputBucketOpts.Name, ts.Tags(), pt.Value(), pt.TS())
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// }
	}
	err = d.db.AddAll(res)
	if err != nil {
		panic(err)
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
			TagName:  "json",
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				toTimeHookFunc(),
			),
			Result: result,
		},
	)
	if err != nil {
		return err
	}

	if err = decoder.Decode(input); err != nil {
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

func Hash(v interface{}) uint64 {
	hash, err := hashstructure.Hash(v, hashstructure.FormatV2, nil)
	if err != nil {
		panic(err)
	}
	return hash
}
