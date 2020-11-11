package monitoring

import (
	"encoding/base64"
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
	tsdb "github.com/nm-morais/demmon/internal/monitoring/TSDB"
	"github.com/nm-morais/demmon/internal/monitoring/metrics_manager"
	"github.com/nm-morais/demmon/internal/monitoring/plugin_manager"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/sirupsen/logrus"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type DemmonConf struct {
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
	pm                     *plugin_manager.PluginManager
	mm                     *metrics_manager.MetricsManager
	fm                     *membership_frontend.MembershipFrontend
}

func New(dConf DemmonConf, babel protocolManager.ProtocolManager) *Demmon {
	db := tsdb.GetDB()
	pm := plugin_manager.New(plugin_manager.PluginManagerConfig{WorkingDir: dConf.PluginDir})
	fm := membership_frontend.New(babel)
	d := &Demmon{
		counter:                1,
		logger:                 logrus.New(),
		nodeUpdatesSubscribers: &sync.Map{},
		conf:                   dConf,
		db:                     db,
		pm:                     pm,
		mm:                     metrics_manager.New(db, pm),
		fm:                     fm,
	}
	setupLogger(d.logger, d.conf.LogFolder, d.conf.LogFile)
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
	var ans interface{}
	var err error

	switch r.Type {
	case routes.GetInView:
		ans, err = d.getInView()
	case routes.MembershipUpdates:
		ans, err = d.subscribeNodeEvents(c)
	case routes.GetRegisteredMetrics:
		ans, err = d.mm.GetRegisteredMetrics()
	case routes.GetRegisteredPlugins:
		ans = d.pm.GetInstalledPlugins()
	case routes.RegisterMetrics:
		reqBody := []*body_types.MetricMetadata{}
		err = mapstructure.Decode(r.Message, &reqBody)
		if err != nil {
			err = errors.New("bad request body type")
			break
		}
		for _, m := range reqBody {
			d.logger.Infof("%+v", m)
		}
		err = d.mm.RegisterMetrics(reqBody)
		if err != nil {
			d.logger.Error(err)
			break
		}
		ans = true
	case routes.PushMetricBlob:
		reqBody := []string{}
		err = mapstructure.Decode(r.Message, &reqBody)
		if err != nil {
			err = errors.New("bad request body type")
			break
		}
		err = d.mm.AddMetricBlob(reqBody)
		if err != nil {
			d.logger.Error(err)
			break
		}

	case routes.AddPlugin:
		reqBody := &body_types.PluginFileBlock{}
		err = mapstructure.Decode(r.Message, reqBody)
		if err != nil {
			d.logger.Error(r.Message)
			d.logger.Error(err)
			err = errors.New("bad request body type")
			break
		}
		err = d.pm.AddPluginChunk(reqBody)
		if err != nil {
			d.logger.Error(err)
			break
		}

	case routes.GetPlugin:
		reqBody := &body_types.GetPluginRequest{}
		err = mapstructure.Decode(r.Message, reqBody)
		if err != nil {
			d.logger.Error(r.Message)
			d.logger.Error(err)
			err = errors.New("bad request body type")
			break
		}

		f, err := d.pm.GetPluginCodeFileReadMode(reqBody.PluginName)
		if err != nil {
			break
		}
		defer d.sendPlugin(f, r, reqBody.PluginName, reqBody.Chunksize, c)
		ans = body_types.NewResponse(r.ID, false, nil, r.Type, nil)

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

func (d *Demmon) sendPlugin(f io.Reader, req *body_types.Request, pluginName string, chunkSize int, c *client) {
	i := 0
	for {
		chunk := make([]byte, chunkSize)
		n, err := f.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Got err", err)
			return
		}
		contentb64 := base64.StdEncoding.EncodeToString(chunk[:n])
		ans := body_types.PluginFileBlock{
			Name:       pluginName,
			FirstBlock: i == 0,
			FinalBlock: false,
			Content:    contentb64,
		}
		fmt.Println("Sending segment ", i)
		c.out <- body_types.NewResponse(req.ID, true, nil, req.Type, ans)
		i++
	}
	ans := &body_types.PluginFileBlock{
		Name:       pluginName,
		FinalBlock: true,
		Content:    "",
	}
	c.out <- body_types.NewResponse(req.ID, true, nil, req.Type, ans)
	fmt.Println("Sent last segment")
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

func setupLogger(logger *logrus.Logger, logFolder, logFile string) {

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
	mw := io.MultiWriter(os.Stdout, file)
	logger.SetOutput(mw)
}
