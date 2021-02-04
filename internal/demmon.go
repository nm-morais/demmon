package internal

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strconv"
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
	"github.com/nm-morais/go-babel/pkg/dataStructures/timedEventQueue"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/reugn/go-quartz/quartz"
	"github.com/sirupsen/logrus"
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
		subID  string
	}
}

type customInterestSetWrapper struct {
	err       error
	nrRetries map[string]int
	clients   map[string]*demmon_client.DemmonClient
	is        body_types.CustomInterestSet
	*sync.Mutex
}

type alarmControl struct {
	id                string
	subID             string
	err               error
	nrRetries         int
	alarm             body_types.InstallAlarmRequest
	lastTimeEvaluated time.Time
	lastTimeTriggered time.Time
	client            *client
	d                 *Demmon
	*sync.Mutex
}

func (ac *alarmControl) ID() string {
	return ac.id
}

func (ac *alarmControl) Notify(interface{}) {
	ac.d.logger.Info("alarm control got notified of insertion in watched timeseries.")
	ac.Lock()
	defer ac.Unlock()
	if time.Since(ac.lastTimeTriggered) > ac.alarm.TriggerBackoffTime &&
		time.Since(ac.lastTimeEvaluated) > ac.alarm.CheckPeriodicity {
		ac.lastTimeEvaluated = time.Now()
		go ac.OnTrigger()
	} else {
		ac.d.logger.Info("alarm not evaluating because time since last evaluation is less than the alarm's minimum periodicity")
	}
}

func (ac *alarmControl) OnTrigger() (bool, *time.Time) {
	ac.Lock()
	defer ac.Unlock()
	ac.lastTimeEvaluated = time.Now()
	if time.Since(ac.lastTimeTriggered) < ac.alarm.TriggerBackoffTime {
		nextTrigger := ac.lastTimeTriggered.Add(ac.alarm.TriggerBackoffTime)
		return true, &nextTrigger
	}
	if time.Since(ac.lastTimeEvaluated) < ac.alarm.CheckPeriodicity {
		nextTrigger := ac.lastTimeEvaluated.Add(ac.alarm.CheckPeriodicity)
		return true, &nextTrigger
	}

	ac.d.logger.Infof("evaluating alarm %d", ac.id)
	res, err := ac.d.me.MakeBoolQuery(ac.alarm.Query.Expression, ac.alarm.Query.Timeout)
	if err != nil {
		ac.d.logger.Errorf("alarm %d failed with error: %s", ac.id, err)
		ac.nrRetries++
		if ac.nrRetries == ac.alarm.MaxRetries {
			ac.d.logger.Errorf("alarm %d has exceeded maxRetries (%d), sending err msg and deleting alarm", ac.id, ac.nrRetries)
			ac.d.sendResponse(body_types.NewResponse(ac.subID, true, nil, 200, routes.InstallAlarm, body_types.AlarmUpdate{
				ID:       ac.id,
				Error:    true,
				Trigger:  false,
				ErrorMsg: err.Error(),
			}), ac.client)

			_, ok := ac.d.alarms.LoadAndDelete(ac.id)
			if ok {
				ac.d.RemoveAlarmWatchlist(ac)
			}
			return false, nil
		}
		nextTrigger := time.Now().Add(ac.alarm.CheckPeriodicity)
		return true, &nextTrigger
	}

	if res == true {
		ac.d.sendResponse(body_types.NewResponse(ac.subID, true, nil, 200, routes.InstallAlarm, body_types.AlarmUpdate{
			ID:       ac.id,
			Error:    false,
			Trigger:  true,
			ErrorMsg: "",
		}), ac.client)
		timeNow := time.Now()
		ac.lastTimeTriggered = timeNow
		nextTrigger := timeNow.Add(ac.alarm.TriggerBackoffTime)
		return true, &nextTrigger
	}
	nextTrigger := time.Now().Add(ac.alarm.CheckPeriodicity)
	return true, &nextTrigger
}

type continuousQueryJobWrapper struct {
	id string
	d  *Demmon
}

func (job continuousQueryJobWrapper) Description() string {
	return ""
}

func (job continuousQueryJobWrapper) Key() int {
	id, _ := strconv.ParseInt(job.id, 10, 64)
	return int(id)
}

func (job continuousQueryJobWrapper) Execute() {
	id, _ := strconv.ParseInt(job.id, 10, 64)
	job.d.handleContinuousQueryTrigger(int(id))
}

type client struct {
	id   uint64
	mu   *sync.Mutex
	conn *websocket.Conn
	done chan interface{}
}

// Description returns a PrintJob description.
func (d *Demmon) sendResponse(resp *body_types.Response, cl *client) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	err := cl.conn.WriteJSON(resp)

	if err != nil {
		d.logger.Errorf("Got error writing to client conn: %s", err.Error())
		if err := cl.conn.Close(); err != nil {
			d.logger.Errorf("Got error closing client conn: %s", err.Error())
		}
	}
}

type Demmon struct {
	alarms                          *sync.Map
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
	alarmTeq                        timedEventQueue.TimedEventQueue
}

func New(
	dConf DemmonConf,
	monitorProto *monitoringProto.Monitor,
	me *engine.MetricsEngine,
	db *tsdb.TSDB,
	fm *membershipFrontend.MembershipFrontend,
	babel protocolManager.ProtocolManager,
) *Demmon {
	logger := logrus.New()
	d := &Demmon{
		alarms:                          &sync.Map{},
		schedulerMu:                     &sync.Mutex{},
		scheduler:                       quartz.NewStdScheduler(),
		customInterestSets:              &sync.Map{},
		continuousQueries:               &sync.Map{},
		continuousQueriesCounter:        new(uint64),
		logger:                          logger,
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
		alarmTeq:                        timedEventQueue.NewTimedEventQueue(logger),
	}

	d.scheduler.Start()
	go d.handleNodeUpdates()
	go d.handleBroadcastMessages()

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
		mu:   &sync.Mutex{},
		done: make(chan interface{}),
	}

	go d.readPump(client)
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
		taskIDStr := fmt.Sprintf("%d", taskID)
		d.continuousQueries.Store(taskIDStr, cc)
		d.schedulerMu.Lock()
		job := &continuousQueryJobWrapper{
			id: taskIDStr,
			d:  d,
		}
		err = d.scheduler.ScheduleJob(job, trigger)

		if err != nil {
			d.schedulerMu.Unlock()
			resp = body_types.NewResponse(r.ID, false, err, 500, r.Type, nil)
			break
		}

		ans := body_types.InstallContinuousQueryReply{
			TaskID: taskIDStr,
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

		setIDNr := utils.GetRandInt(math.MaxInt64)
		setID := fmt.Sprintf("%d", setIDNr)
		customIntSet := &customInterestSetWrapper{
			nrRetries: make(map[string]int),
			is:        reqBody,
			Mutex:     &sync.Mutex{},
			clients:   make(map[string]*demmon_client.DemmonClient),
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
		customISGeneric, ok := d.customInterestSets.Load(reqBody.SetID)
		if !ok {
			resp = body_types.NewResponse(r.ID, false, body_types.ErrCustomInterestSetNotFound, 404, r.Type, nil)
			break
		}
		customIS := customISGeneric.(*customInterestSetWrapper)
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
				subID  string
			}{
				{client: c, subID: r.ID},
			},
		})

		if loaded {
			actual := actualGeneric.(*broadcastMessageSubscribers)
			actual.Lock()
			actual.subs = append(actual.subs, &struct {
				client *client
				subID  string
			}{client: c, subID: r.ID})
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
		alarmIDNr := utils.GetRandInt(math.MaxInt64)
		alarmID := fmt.Sprintf("%d", alarmIDNr)

		alarm := &alarmControl{
			id:                alarmID,
			err:               nil,
			nrRetries:         0,
			alarm:             *reqBody,
			Mutex:             &sync.Mutex{},
			d:                 d,
			lastTimeEvaluated: time.Time{},
			lastTimeTriggered: time.Time{},
			client:            c,
			subID:             r.ID,
		}

		d.alarms.Store(alarmID, alarm)

		err := d.installAlarmWatchlist(alarm, reqBody.WatchList)
		if err != nil {
			resp = body_types.NewResponse(r.ID, false, err, 404, r.Type, nil)
			break
		}

		if alarm.alarm.CheckPeriodic {
			d.alarmTeq.Add(alarm, time.Now())
		}

		d.logger.Infof("Added new alarm: %+v", reqBody)
		resp = body_types.NewResponse(r.ID, false, nil, 200, r.Type, body_types.InstallAlarmReply{ID: alarmID})
	case routes.RemoveAlarm:
		reqBody := &body_types.RemoveResourceRequest{}
		if !d.extractBody(r, reqBody, resp) {
			break
		}
		alarmID := reqBody.ResourceID
		alarmGeneric, ok := d.alarms.LoadAndDelete(alarmID)
		if ok {
			alarm := alarmGeneric.(*alarmControl)

			if alarm.alarm.CheckPeriodic {
				ok = d.alarmTeq.Remove(alarm.ID())
				if !ok {
					panic("was installed but not in timer queue")
				}
			}

			err := d.RemoveAlarmWatchlist(alarm)
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
	d.sendResponse(resp, c)
}

func (d *Demmon) subscribeNodeEvents(r *body_types.Request, c *client) body_types.NodeUpdateSubscriptionResponse {
	d.nodeUpdatesSubscribers.Store(r.ID, c)
	return body_types.NodeUpdateSubscriptionResponse{
		View: d.fm.GetInView(),
	}
}

func (d *Demmon) handleBroadcastMessages() {
	msgChan := d.fm.GetBroadcastChan()
	updates := []body_types.Message{}

	handleUpdateFunc := func(nextUpdate interface{}) {
		update := (nextUpdate).(body_types.Message)
		updates = append(updates, update)
	}

	for {

		if len(updates) == 0 {
			handleUpdateFunc(<-msgChan)
		}

		select {
		case v := <-msgChan:
			handleUpdateFunc(v)
		default:
			for _, bcastMsg := range updates {
				subsGeneric, ok := d.broadcastMessageSubscribers.Load(bcastMsg.ID)
				if !ok {
					d.logger.Warnf("Could not deliver broadcasted messages because there are no listeners for msg type %d", bcastMsg.ID)
					continue
				}
				subs := subsGeneric.(*broadcastMessageSubscribers)
				subs.Lock()

				for _, cl := range subs.subs {
				repeat:
					select {
					default:
						d.sendResponse(body_types.NewResponse(cl.subID, true, nil, 200, routes.InstallBroadcastMessageHandler, body_types.Message{
							ID:      bcastMsg.ID,
							TTL:     bcastMsg.TTL,
							Content: bcastMsg.Content,
						}), cl.client)
						d.logger.Infof("Delivered Bcast message to client: %+v", bcastMsg)
					case <-cl.client.done:
						for idx, sub := range subs.subs {
							if sub.client.id == cl.client.id {
								subs.subs = append(subs.subs[:idx], subs.subs[idx+1:]...)
								break
							}
						}
					case v := <-msgChan:
						handleUpdateFunc(v)
						goto repeat
					}

				}
				subs.Unlock()
			}
			updates = []body_types.Message{}
		}
	}
}

func (d *Demmon) handleNodeUpdates() {
	nodeUps, nodeDowns := d.fm.MembershipUpdates()
	for {
		select {
		case nodeUp := <-nodeUps:
			d.logger.Infof("Delivering node up %+v", nodeUp)
			d.nodeUpdatesSubscribers.Range(func(key, valueGeneric interface{}) bool {
				subID := key.(string)
				client := valueGeneric.(*client)
				d.sendResponse(body_types.NewResponse(subID, true, nil, 200, routes.MembershipUpdates, nodeUp), client)
				return true
			})
		case nodeDown := <-nodeDowns:
			d.logger.Infof("Delivering node down %+v", nodeDown)
			d.nodeUpdatesSubscribers.Range(func(key, valueGeneric interface{}) bool {
				subID := key.(string)
				client := valueGeneric.(*client)
				d.sendResponse(body_types.NewResponse(subID, true, nil, 200, routes.MembershipUpdates, nodeDown), client)
				return true
			})
		}
	}
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

func (d *Demmon) RemoveAlarmWatchlist(alarm *alarmControl) error {
	for _, toWatch := range alarm.alarm.WatchList {
		b, ok := d.db.GetBucket(toWatch.MeasurementName)
		if ok {
			b.RemoveWatchlist(alarm)
		}
	}
	return nil
}

func (d *Demmon) handleCustomInterestSet(taskID string, req *body_types.Request, c *client) {
	defer d.logger.Warnf("Custom interest set %s returning", taskID)
	defer d.customInterestSets.Delete(taskID)

	jobGeneric, ok := d.customInterestSets.Load(taskID)
	if !ok {
		return
	}

	customJobWrapper := jobGeneric.(*customInterestSetWrapper)
	ticker := time.NewTicker(customJobWrapper.is.IS.OutputBucketOpts.Granularity.Granularity)
	wg := &sync.WaitGroup{}

	for range ticker.C {
		d.logger.Infof("Custom interest set %s trigger", taskID)
		jobGeneric, ok = d.customInterestSets.Load(taskID)
		if !ok {
			return
		}
		job := jobGeneric.(*customInterestSetWrapper)
		if job.err != nil {
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
					err, _ := newCL.ConnectTimeout(job.is.DialTimeout)
					if err != nil {
						d.logger.Errorf("Got error %s connecting to node %s in custom interest set %s", err.Error(), p.IP.String(), taskID)
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
							d.logger.Errorf("Could not connect to custom interest set %s target: %s ", taskID, p.IP.String())
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
			d.logger.Errorf("returning from interest set %s due to error %s", taskID, job.err.Error())
			d.sendResponse(body_types.NewResponse(req.ID, true, nil, 500, routes.InstallCustomInterestSet, body_types.CustomInterestSetErr{Err: body_types.ErrCannotConnect.Error()}), c)
			return
		}
		for _, p := range customJobWrapper.is.Hosts {
			cl, ok := job.clients[p.IP.String()]
			if !ok {
				continue
			}
			clCopy := cl
			pCopy := p

			wg.Add(1)

			go func(cl *demmon_client.DemmonClient, p body_types.CustomInterestSetHost) {
				defer wg.Done()
				res, err := cl.Query(query.Expression, query.Timeout)
				if err != nil {
					d.logger.Errorf("Got error %s querying  node %s in custom interest set %s", err.Error(), p.IP.String(), taskID)
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
			d.logger.Errorf("returning from interest set %s due to error %s", taskID, job.err.Error())
			d.sendResponse(body_types.NewResponse(req.ID, true, nil, 500, routes.InstallCustomInterestSet, body_types.CustomInterestSetErr{Err: body_types.ErrQuerying.Error()}), c)
			return
		}
		job.Lock()
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
		job.Unlock()
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
		if job.triedNr == job.IS.NrRetries {
			d.logger.Errorf("Removing continous query: %s", taskID)
			d.schedulerMu.Lock()
			defer d.schedulerMu.Unlock()
			err := d.scheduler.DeleteJob(taskID)
			if err != nil {
				d.logger.Errorf("Failed to delete continuous query %s: %s", taskID, err.Error())
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
