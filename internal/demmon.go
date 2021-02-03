package internal

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	demmon_client "github.com/nm-morais/demmon-client/pkg"
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/routes"
	membershipFrontend "github.com/nm-morais/demmon/internal/membership/frontend"
	"github.com/nm-morais/demmon/internal/monitoring/engine"
	monitoringProto "github.com/nm-morais/demmon/internal/monitoring/protocol"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/demmon/internal/utils"
	priorityqueue "github.com/nm-morais/go-babel/pkg/dataStructures/priorityQueue"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/reugn/go-quartz/quartz"
	"github.com/sirupsen/logrus"
	"github.com/ungerik/go-dry"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

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
	mu          *sync.Mutex
	IS          body_types.InstallContinuousQueryRequest
	description string
	err         error
	triedNr     int
	lastRan     time.Time
}

type broadcastMessageSubscribers struct {
	*sync.Mutex
	subs []*struct {
		client *client
		subID  int
	}
}

type customInterestSetWrapper struct {
	err       error
	nrRetries map[string]int
	clients   map[string]*demmon_client.DemmonClient
	is        body_types.CustomInterestSet
	*dry.DebugMutex
}

type alarmControl struct {
	id                int64
	subId             uint64
	err               error
	nrRetries         int
	resetChan         chan bool
	alarm             body_types.InstallAlarmRequest
	lastTimeEvaluated time.Time
	lastTimeTriggered time.Time
	client            *client
	d                 *Demmon
	*sync.Mutex
}

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
	alarms                          *sync.Map
	addAlarmChan                    chan *alarmControl
	schedulerMu                     *sync.Mutex
	scheduler                       quartz.Scheduler
	customInterestSets              *sync.Map
	continuousQueries               *sync.Map
	continuousQueriesCounter        *uint64
	logger                          *logrus.Logger
	counter                         *uint64
	conf                            DemmonConf
	db                              *tsdb.TSDB
	nodeUpdatesSubscribers          *sync.Map
	broadcastMessageSubscribersLock *sync.Mutex
	broadcastMessageSubscribers     *sync.Map
	monitorProto                    *monitoringProto.Monitor
	fm                              *membershipFrontend.MembershipFrontend
	me                              *engine.MetricsEngine
	babel                           protocolManager.ProtocolManager
}

func New(
	dConf DemmonConf,
	monitorProto *monitoringProto.Monitor,
	me *engine.MetricsEngine,
	db *tsdb.TSDB,
	fm *membershipFrontend.MembershipFrontend,
	babel protocolManager.ProtocolManager,
) *Demmon {
	d := &Demmon{
		alarms:                          &sync.Map{},
		addAlarmChan:                    make(chan *alarmControl),
		schedulerMu:                     &sync.Mutex{},
		scheduler:                       quartz.NewStdScheduler(),
		customInterestSets:              &sync.Map{},
		continuousQueries:               &sync.Map{},
		continuousQueriesCounter:        new(uint64),
		logger:                          logrus.New(),
		counter:                         new(uint64),
		conf:                            dConf,
		db:                              db,
		nodeUpdatesSubscribers:          &sync.Map{},
		broadcastMessageSubscribersLock: &sync.Mutex{},
		broadcastMessageSubscribers:     &sync.Map{},
		monitorProto:                    monitorProto,
		fm:                              fm,
		me:                              me,
		babel:                           babel,
	}

	d.scheduler.Start()
	go d.handleNodeUpdates()
	go d.handleBroadcastMessages()
	go d.evalAlarmsPeriodic()

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

	var resp = &body_types.Response{}

	switch r.Type {
	case routes.GetInView:
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, d.fm.GetInView())
	case routes.MembershipUpdates:
		ans := d.subscribeNodeEvents(r, c)
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, ans)
	case routes.GetRegisteredMetricBuckets:
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, d.db.GetRegisteredBuckets())
	case routes.InstallBucket:
		reqBody := body_types.BucketOptions{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		_, err := d.db.CreateBucket(reqBody.Name, reqBody.Granularity.Granularity, reqBody.Granularity.Count)
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
		reqBody := []body_types.TimeseriesDTO{}

		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		d.logger.Infof("Adding: %+v, type:%s", r.Message, reflect.TypeOf(r.Message))
		tsArr := make([]tsdb.ReadOnlyTimeSeries, 0, len(reqBody))
		for _, ts := range reqBody {
			tsArr = append(tsArr, tsdb.StaticTimeseriesFromDTO(ts))
		}
		err := d.db.AddAll(tsArr)
		if err != nil {
			d.logger.Errorf("Got error adding metric blob: %s", err.Error())
			if errors.Is(err, tsdb.ErrBucketNotFound) {
				resp = body_types.NewResponse(r.ID, false, err, 404, r.Type, nil)
				break
			}
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
			break
		}

		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, nil)
	case routes.Query:
		reqBody := body_types.QueryRequest{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		queryResult, err := d.me.MakeQuery(reqBody.Query.Expression, reqBody.Query.Timeout)
		if err != nil {
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
			break
		}

		toReturn := make([]body_types.TimeseriesDTO, 0, len(queryResult))
		for _, ts := range queryResult {
			toReturn = append(toReturn, ts.ToDTO())
		}
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, toReturn)
	case routes.InstallContinuousQuery:
		reqBody := body_types.InstallContinuousQueryRequest{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		_, err := d.db.CreateBucket(
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
			mu:          &sync.Mutex{},
			description: reqBody.Description,
			err:         nil,
			triedNr:     reqBody.NrRetries,
			IS:          reqBody,
		}
		d.continuousQueries.Store(int(taskID), cc)
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
						TaskID:    key.(int),
						Error:     job.err,
						LastRan:   job.lastRan,
						CurrTry:   job.triedNr,
						NrRetries: job.IS.NrRetries,
					},
				)
				job.mu.Unlock()
				return true
			},
		)
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, ans)
	case routes.InstallCustomInterestSet:
		reqBody := body_types.CustomInterestSet{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		_, err := d.db.CreateBucket(
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

		setID := utils.GetRandInt(math.MaxInt64)
		customIntSet := &customInterestSetWrapper{
			nrRetries:  make(map[string]int),
			is:         reqBody,
			DebugMutex: &dry.DebugMutex{},
			clients:    make(map[string]*demmon_client.DemmonClient),
		}

		d.logger.Infof("Creating custom interest set %d func output bucket: %s", setID, reqBody.IS.OutputBucketOpts.Name)
		d.customInterestSets.Store(setID, customIntSet)

		go d.handleCustomInterestSet(setID, r, c)
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, body_types.InstallInterestSetReply{SetID: setID})

	case routes.RemoveCustomInterestSet:
		reqBody := body_types.RemoveResourceRequest{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}
		d.customInterestSets.Delete(reqBody.ResourceID)
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, nil)
	case routes.UpdateCustomInterestSetHosts:
		reqBody := body_types.UpdateCustomInterestSetReq{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}
		d.logger.Infof("Updating custom interest set %d", reqBody.SetID)

		customISGeneric, ok := d.customInterestSets.Load(reqBody.SetID)
		if !ok {
			d.logger.Errorf("custom interest set %d not found", reqBody.SetID)
			resp = body_types.NewResponse(r.ID, false, body_types.ErrCustomInterestSetNotFound, 404, r.Type, nil)
			break
		}

		customIS := customISGeneric.(*customInterestSetWrapper)
		d.logger.Infof("Locking custom interest set lock %d", reqBody.SetID)
		customIS.Lock()
		customIS.is.Hosts = reqBody.Hosts
		customIS.Unlock()
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, nil)
	case routes.InstallNeighborhoodInterestSet:
		reqBody := body_types.NeighborhoodInterestSet{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		d.logger.Infof("Creating neigh interest set func output bucket: %s", reqBody.IS.OutputBucketOpts.Name)

		_, err := d.db.CreateBucket(
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
		d.monitorProto.AddNeighborhoodInterestSetReq(int64(neighSetID), reqBody)
		d.logger.Infof("Added new neighborhood interest set: %+v", reqBody)
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, neighSetID)

	case routes.InstallTreeAggregationFunction:
		reqBody := body_types.TreeAggregationSet{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		d.logger.Infof("Creating tree aggregation func output bucket: %s", reqBody.OutputBucketOpts.Name)

		_, err := d.db.CreateBucket(
			reqBody.OutputBucketOpts.Name,
			reqBody.OutputBucketOpts.Granularity.Granularity,
			reqBody.OutputBucketOpts.Granularity.Count,
		)

		if err != nil {
			d.logger.Errorf("Got error installing tree aggregation function: %s", err.Error())
			if errors.Is(err, tsdb.ErrAlreadyExists) {
				resp = body_types.NewResponse(r.ID, false, err, 409, r.Type, nil)
				break
			}
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
			break
		}

		treeSetID := utils.GetRandInt(math.MaxInt64)
		d.monitorProto.AddTreeAggregationFuncReq(treeSetID, reqBody)
		d.logger.Infof("Added new tree aggregation function: %+v", reqBody)
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, treeSetID)
	case routes.InstallGlobalAggregationFunction:
		reqBody := body_types.GlobalAggregationFunction{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		d.logger.Infof("Creating neigh interest set func output bucket: %s", reqBody.OutputBucketOpts.Name)

		_, err := d.db.CreateBucket(
			reqBody.OutputBucketOpts.Name,
			reqBody.OutputBucketOpts.Granularity.Granularity,
			reqBody.OutputBucketOpts.Granularity.Count,
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

		neighSetID := Hash(reqBody)
		d.monitorProto.AddGlobalAggregationFuncReq(int64(neighSetID), reqBody)
		d.logger.Infof("Added new neighborhood interest set: %+v", reqBody)
		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, neighSetID)
	case routes.BroadcastMessage:
		reqBody := body_types.Message{}
		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		err := d.fm.BroadcastMessage(reqBody)
		if err != nil {
			resp = body_types.NewResponse(r.ID, false, err, 400, r.Type, nil)
			break
		}

		resp = body_types.NewResponse(r.ID, false, err, 200, r.Type, nil)
	case routes.InstallBroadcastMessageHandler:
		reqBody := body_types.InstallMessageHandlerRequest{}

		if !d.extractBody(r, &reqBody, resp) {
			break
		}

		actualGeneric, loaded := d.broadcastMessageSubscribers.LoadOrStore(reqBody.ID, &broadcastMessageSubscribers{
			Mutex: &sync.Mutex{},
			subs: []*struct {
				client *client
				subID  int
			}{
				{client: c, subID: int(r.ID)},
			},
		})

		if loaded {
			actual := actualGeneric.(*broadcastMessageSubscribers)
			actual.Lock()
			actual.subs = append(actual.subs, &struct {
				client *client
				subID  int
			}{client: c, subID: int(r.ID)})
			actual.Unlock()
		}
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, nil)
	case routes.StartBabel:
		d.babel.StartAsync()
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, nil)
	case routes.InstallAlarm:
		reqBody := &body_types.InstallAlarmRequest{}
		if !d.extractBody(r, reqBody, resp) {
			break
		}
		alarmID := utils.GetRandInt(math.MaxInt64)
		alarm := &alarmControl{
			id:                alarmID,
			err:               nil,
			nrRetries:         0,
			resetChan:         make(chan bool, 1),
			alarm:             *reqBody,
			Mutex:             &sync.Mutex{},
			d:                 d,
			lastTimeEvaluated: time.Time{},
			lastTimeTriggered: time.Time{},
			client:            c,
			subId:             r.ID,
		}

		d.alarms.Store(alarmID, alarm)

		err := d.installAlarmWatchlist(alarm, reqBody.WatchList)
		if err != nil {
			resp = body_types.NewResponse(r.ID, false, err, 404, r.Type, nil)
			break
		}

		if alarm.alarm.CheckPeriodic {
			d.addAlarmToEvalPeriodic(alarm)
		}

		d.logger.Infof("Added new alarm: %+v", reqBody)
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, body_types.InstallAlarmReply{ID: alarmID})
	case routes.RemoveAlarm:
		reqBody := &body_types.RemoveResourceRequest{}
		if !d.extractBody(r, reqBody, resp) {
			break
		}
		alarmID := reqBody.ResourceID
		alarm, ok := d.alarms.LoadAndDelete(alarmID)
		if ok {
			err := d.removeAlarmWatchlist(alarm.(*alarmControl))
			if err != nil {
				panic(err)
			}
		}

	default:
		resp = body_types.NewResponse(r.ID, false, body_types.ErrNonRecognizedOp, 400, r.Type, nil)
	}

	if resp.Error {
		d.logger.Errorf("Got request %s, response: status: %d, err: %s", r.Type.String(), resp.Code, resp.GetMsgAsErr().Error())
	}

	// else {
	// 	d.logger.Infof("Got request %s, response: status:% d, response: %+v", r.Type.String(), resp.Code, resp.Message)
	// }

	select {
	case c.out <- resp:
	case <-time.After(DeliverRequestResponseTimeout):
		d.logger.Panic("TIMEOUT: Could not deliver response")
	}
}

func (d *Demmon) subscribeNodeEvents(r *body_types.Request, c *client) body_types.NodeUpdateSubscriptionResponse {
	d.nodeUpdatesSubscribers.Store(r.ID, c)
	return body_types.NodeUpdateSubscriptionResponse{
		View: d.fm.GetInView(),
	}
}

func (d *Demmon) handleBroadcastMessages() {
	msgChan := d.fm.GetBroadcastChan()
	for bcastMsg := range msgChan {
		msgCopy := bcastMsg
		d.logger.Infof("Delivering Bcast message: %+v", bcastMsg)

		subsGeneric, ok := d.broadcastMessageSubscribers.Load(bcastMsg.ID)
		if !ok {
			d.logger.Warnf("Could not deliver broadcasted messages because there are no listeners for msg type %d", bcastMsg.ID)
			continue
		}
		subs := subsGeneric.(*broadcastMessageSubscribers)
		subs.Lock()
		for _, cl := range subs.subs {
			cl.client.out <- body_types.NewResponse(uint64(cl.subID), true, nil, 200, routes.InstallBroadcastMessageHandler, body_types.Message{
				ID:      msgCopy.ID,
				TTL:     msgCopy.TTL,
				Content: msgCopy.Content,
			})
		}
		subs.Unlock()
	}
}

func (d *Demmon) handleNodeUpdates() {
	nodeUps, nodeDowns := d.fm.MembershipUpdates()
	for {
		select {
		case nodeUp := <-nodeUps:
			d.logger.Infof("Delivering node up %+v", nodeUp)
			d.nodeUpdatesSubscribers.Range(func(key, valueGeneric interface{}) bool {
				subID := key.(uint64)
				client := valueGeneric.(*client)
				client.out <- body_types.NewResponse(subID, true, nil, 200, routes.MembershipUpdates, nodeUp)
				return true
			})
		case nodeDown := <-nodeDowns:
			d.logger.Infof("Delivering node down %+v", nodeDown)
			d.nodeUpdatesSubscribers.Range(func(key, valueGeneric interface{}) bool {
				subID := key.(uint64)
				client := valueGeneric.(*client)
				client.out <- body_types.NewResponse(subID, true, nil, 200, routes.MembershipUpdates, nodeDown)
				return true
			})
		}
	}
}

func (ac *alarmControl) GetID() string {
	return fmt.Sprintf("%d", ac.id)
}

func (d *Demmon) installAlarmWatchlist(observer utils.Observer, watchList []body_types.TimeseriesFilter) error {
	for _, toWatch := range watchList {
		_, ok := d.db.GetBucket(toWatch.MeasurementName)
		if !ok {
			return body_types.ErrBucketNotFound
		}
	}

	for _, toWatch := range watchList {
		b, _ := d.db.GetBucket(toWatch.MeasurementName)
		b.RegisterWatchlist(observer, toWatch)
	}

	return nil
}

func (d *Demmon) removeAlarmWatchlist(alarm *alarmControl) error {
	for _, toWatch := range alarm.alarm.WatchList {
		b, ok := d.db.GetBucket(toWatch.MeasurementName)
		if ok {
			b.RemoveWatchlist(alarm)
		}
	}
	return nil
}

func (ac *alarmControl) Notify(interface{}) {
	ac.d.logger.Info("alarm control got notified of insertion in watched timeseries.")
	ac.Lock()
	defer ac.Unlock()
	if time.Since(ac.lastTimeTriggered) > ac.alarm.TriggerBackoffTime &&
		time.Since(ac.lastTimeEvaluated) > ac.alarm.CheckPeriodicity {
		ac.lastTimeEvaluated = time.Now()
		go ac.d.evalAlarm(ac)
		if ac.alarm.CheckPeriodic {
			select {
			case ac.resetChan <- true:
			default:
			}
		}
	} else {
		ac.d.logger.Info("alarm not evaluating because time since last evaluation is less than the alarm's minimum periodicity")
	}
}

func (d *Demmon) addAlarmToEvalPeriodic(alarm *alarmControl) {
	d.addAlarmChan <- alarm
}

func (d *Demmon) evalAlarm(alarm *alarmControl) {
	alarm.Lock()
	defer alarm.Unlock()
	d.logger.Infof("evaluating alarm %d", alarm.id)
	res, err := d.me.MakeBoolQuery(alarm.alarm.Query.Expression, alarm.alarm.Query.Timeout)
	if err != nil {
		d.logger.Errorf("alarm %d failed with error: %s", alarm.id, err)
		alarm.nrRetries++
		if alarm.nrRetries == alarm.alarm.MaxRetries {
			d.logger.Errorf("alarm %d has exceeded maxRetries (%d), sending err msg and deleting alarm", alarm.id, alarm.nrRetries)
			alarm.client.out <- body_types.NewResponse(alarm.subId, true, nil, 200, routes.InstallAlarm, body_types.AlarmUpdate{
				ID:       alarm.id,
				Error:    true,
				Trigger:  false,
				ErrorMsg: err.Error(),
			})
			alarm, ok := d.alarms.LoadAndDelete(alarm.id)
			if ok {
				d.removeAlarmWatchlist(alarm.(*alarmControl))
			}
		}
		return
	}

	if res == true {
		alarm.client.out <- body_types.NewResponse(alarm.subId, true, nil, 200, routes.InstallAlarm, body_types.AlarmUpdate{
			ID:       alarm.id,
			Error:    false,
			Trigger:  true,
			ErrorMsg: "",
		})
		alarm.lastTimeTriggered = time.Now()
		return
	}
}

func (d *Demmon) evalAlarmsPeriodic() {
	var t *time.Timer
	pq := priorityqueue.PriorityQueue{}

	addAlarmToQueue := func(alarm *alarmControl, nextTrigger time.Time) {
		d.logger.Infof("added alarm control %d to pq", alarm.id)
		pqItem := &priorityqueue.Item{
			Value:    alarm,
			Priority: nextTrigger.UnixNano(),
		}
		heap.Push(&pq, pqItem)
		heap.Init(&pq)
	}

	getNextFromQueue := func() *alarmControl {
		for len(pq) > 0 {
			nextItem := heap.Pop(&pq).(*priorityqueue.Item).Value.(*alarmControl)
			alarmInt, stillActive := d.alarms.Load(nextItem.id)
			if !stillActive {
				d.logger.Warnf("alarm deleted: %d", nextItem.id)
				continue
			}
			// drain reset channel
			alarm := alarmInt.(*alarmControl)
			select {
			case <-alarm.resetChan:
				addAlarmToQueue(nextItem, alarm.lastTimeEvaluated.Add(alarm.alarm.CheckPeriodicity))
			default:
				return alarm
			}
		}
		return nil
	}

	for {

		alarm := getNextFromQueue()
		if alarm == nil {
			alarm = <-d.addAlarmChan
			addAlarmToQueue(alarm, time.Now().Add(alarm.alarm.CheckPeriodicity))
			continue
		}

		// control loop
		t = time.NewTimer(time.Until(alarm.lastTimeEvaluated.Add(alarm.alarm.CheckPeriodicity)))
		select {
		case newAlarm := <-d.addAlarmChan:
			addAlarmToQueue(newAlarm, time.Now().Add(newAlarm.alarm.CheckPeriodicity))
			addAlarmToQueue(alarm, alarm.lastTimeEvaluated.Add(alarm.alarm.CheckPeriodicity))
		case <-t.C:
			alarmInt, stillActive := d.alarms.Load(alarm.id)
			if !stillActive {
				d.logger.Infof("alarm deleted meanwhile: %d", alarm.id)
				break
			}
			alarm := alarmInt.(*alarmControl)
			alarm.Lock()
			alarm.lastTimeEvaluated = time.Now()
			if time.Since(alarm.lastTimeTriggered) > alarm.alarm.TriggerBackoffTime {
				go d.evalAlarm(alarm)
			}
			addAlarmToQueue(alarm, alarm.lastTimeEvaluated.Add(alarm.alarm.CheckPeriodicity))
			alarm.Unlock()
		case <-alarm.resetChan:
			_, stillActive := d.alarms.Load(alarm.id)
			if !stillActive {
				d.logger.Infof("alarm deleted: %d", alarm.id)
				break
			}
			alarm.Lock()
			addAlarmToQueue(alarm, alarm.lastTimeEvaluated.Add(alarm.alarm.CheckPeriodicity))
			alarm.Unlock()
		}
		t.Stop()
	}
}

func (d *Demmon) handleCustomInterestSet(taskID int64, req *body_types.Request, c *client) {
	defer d.logger.Warnf("Custom interest set %d returning", taskID)
	jobGeneric, ok := d.customInterestSets.Load(taskID)
	if !ok {
		return
	}
	defer d.customInterestSets.Delete(taskID)

	customJobWrapper := jobGeneric.(*customInterestSetWrapper)
	ticker := time.NewTicker(customJobWrapper.is.IS.OutputBucketOpts.Granularity.Granularity)
	wg := &sync.WaitGroup{}

	for range ticker.C {
		d.logger.Infof("Custom interest set %d trigger", taskID)
		jobGeneric, ok = d.customInterestSets.Load(taskID)
		if !ok {
			return
		}
		job := jobGeneric.(*customInterestSetWrapper)
		if job.err != nil { // TODO should it cancel?
			return
		}
		query := job.is.IS.Query

		for _, p := range customJobWrapper.is.Hosts {
			_, ok := job.clients[p.IP.String()]
			if ok {
				d.logger.Infof("Already have client for peer %s", p.IP.String())
				continue
			}

			wg.Add(1)
			pCopy := p

			go func(p body_types.CustomInterestSetHost) {
				defer wg.Done()
				newCL := demmon_client.New(demmon_client.DemmonClientConf{
					DemmonPort:     p.Port,
					DemmonHostAddr: p.IP.String(),
					RequestTimeout: customJobWrapper.is.IS.Query.Timeout,
				})

				for i := 0; i < customJobWrapper.is.IS.MaxRetries; i++ {
					err := newCL.ConnectTimeout(job.is.DialTimeout)
					if err != nil {
						d.logger.Errorf("Got error %s connecting to node %s in custom interest set %d", err.Error(), p.IP.String(), taskID)
						job.Lock()
						nrRetries, ok := job.nrRetries[p.IP.String()]
						if !ok {
							job.nrRetries[p.IP.String()] = 0
							nrRetries = 0
						}
						job.Unlock()
						if nrRetries == customJobWrapper.is.IS.MaxRetries {
							job.Lock()
							job.err = err
							job.Unlock()
							d.logger.Errorf("Could not connect to custom interest set %d target: %s ", taskID, p.IP.String())
							return
						}
						job.Lock()
						job.nrRetries[p.IP.String()]++
						job.Unlock()
						time.Sleep(customJobWrapper.is.DialRetryBackoff * time.Duration(i))
						continue
					}
					d.logger.Infof("Connected to custom interest set %d target: %s successfully", taskID, p.IP.String())
					job.Lock()
					job.nrRetries[p.IP.String()] = 0
					job.clients[p.IP.String()] = newCL
					job.Unlock()
					break
				}
			}(pCopy)
		}
		wg.Wait()
		if job.err != nil {
			d.logger.Errorf("returning from interest set %d due to error %s", taskID, job.err.Error())
			c.out <- body_types.NewResponse(req.ID, true, body_types.ErrCannotConnect, 500, routes.InstallCustomInterestSet, nil)
			return
		}
		for _, p := range customJobWrapper.is.Hosts {
			cl, ok := job.clients[p.IP.String()]
			if !ok {
				panic("client is nil")
			}
			clCopy := cl
			pCopy := p

			wg.Add(1)

			go func(cl *demmon_client.DemmonClient, p body_types.CustomInterestSetHost) {
				defer wg.Done()
				res, err := cl.Query(query.Expression, query.Timeout)
				if err != nil {
					job.Lock()
					nrRetries, ok := job.nrRetries[p.IP.String()]
					if !ok {
						job.nrRetries[p.IP.String()] = 0
						nrRetries = 0
					}
					job.Unlock()
					if nrRetries == customJobWrapper.is.IS.MaxRetries {
						job.Lock()
						job.err = err
						job.Unlock()
						return
					}
					job.Lock()
					job.nrRetries[p.IP.String()]++
					job.Unlock()
					return
				}

				toAdd := []tsdb.ReadOnlyTimeSeries{}
				for _, ts := range res {
					ts.MeasurementName = job.is.IS.OutputBucketOpts.Name
					tmp := tsdb.StaticTimeseriesFromDTO(ts)
					toAdd = append(toAdd, tmp)
				}
				d.logger.Infof("Custom interest set %d adding to DB values: %+v", taskID, toAdd)
				err = d.db.AddAll(toAdd)
				if err != nil {
					d.logger.Panicf("Unexpected err adding metric to db: %s", err.Error())
					return
				}
			}(clCopy, pCopy)
		}
		wg.Wait()
		if job.err != nil {
			d.logger.Errorf("returning from interest set %d due to error %s", taskID, job.err.Error())
			c.out <- body_types.NewResponse(req.ID, true, body_types.ErrQuerying, 500, routes.InstallCustomInterestSet, nil)
			return
		}

		for host, cl := range job.clients {
			found := false
			for _, h := range job.is.Hosts {
				if host == h.IP.String() {
					found = true
					break
				}
			}
			if !found {
				d.logger.Warnf("Continuous query removing client %s because it is not present in hosts array", host)
				cl.Disconnect()
				delete(job.clients, host)
			}
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
	res, err := d.me.MakeQuery(job.IS.Expression, job.IS.ExpressionTimeout)
	if err != nil {
		d.logger.Errorf("Continuous query %d failed with error: %s", taskID, err)
		job.triedNr++
		job.err = err
		if job.triedNr == job.triedNr {
			d.schedulerMu.Lock()
			defer d.schedulerMu.Unlock()
			err := d.scheduler.DeleteJob(taskID)
			if err != nil {
				d.logger.Errorf("Failed to delete continuous query: %s", err.Error())
			}
			return
		}
		return
	}

	for _, ts := range res {
		ts.(*tsdb.StaticTimeseries).SetName(job.IS.OutputBucketOpts.Name)

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
