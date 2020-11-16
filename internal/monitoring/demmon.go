package monitoring

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon-common/routes"
	"github.com/nm-morais/demmon/internal/membership/membership_frontend"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
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

type client struct {
	id   uint64
	conn *websocket.Conn
	out  chan *body_types.Response
}

type Demmon struct {
	logger                 *logrus.Logger
	counter                uint64
	conf                   DemmonConf
	db                     *tsdb.TSDB
	nodeUpdatesSubscribers *sync.Map
	fm                     *membership_frontend.MembershipFrontend
}

func New(dConf DemmonConf, babel protocolManager.ProtocolManager) *Demmon {
	db := tsdb.GetDB()
	fm := membership_frontend.New(babel)
	d := &Demmon{
		counter:                1,
		logger:                 logrus.New(),
		nodeUpdatesSubscribers: &sync.Map{},
		conf:                   dConf,
		db:                     db,
		fm:                     fm,
	}
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
		id:   atomic.AddUint64(&d.counter, 1),
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
		reqBody := body_types.PointCollection{}
		err = mapstructure.Decode(r.Message, &reqBody)
		if err != nil {
			d.logger.Error(err)
			err = errors.New("bad request body type")
			break
		}
		for _, m := range reqBody {
			d.db.AddMetric(m.Name, m.Tags, m.Fields, time.Unix(0, m.TS))
			samples := d.db.GetOrCreateBucket(m.Name).GetOrCreateTimeseries(m.Tags).All()
			d.logger.Info("Samples in timeseries:")
			for _, s := range samples {
				d.logger.Infof("%s : %+v", s.TS, s.Fields)
			}
		}
		// err = d.db.AddMetric().AddMetricBlob(reqBody)
		if err != nil {
			d.logger.Error(err)
			break
		}
	case routes.QueryMetric:
		panic("not yet implemented")
	case routes.IsMetricActive:
		panic("not yet implemented")
	case routes.BroadcastMessage:
		panic("not yet implemented")
	case routes.AlarmTrigger:
		panic("not yet implemented")
	default:
		err = errors.New("non-recognized operation")
	}

	d.logger.Infof("Got request %s, response: err:%+v, response:%+v", rType.String(), err, ans)
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
