package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"math/big"
	"net"
	"runtime"
	"time"

	client "github.com/nm-morais/demmon-client/pkg"
	exporter "github.com/nm-morais/demmon-exporter"
	membershipFrontend "github.com/nm-morais/demmon/internal/membership/frontend"
	membershipProtocol "github.com/nm-morais/demmon/internal/membership/protocol"
	"github.com/nm-morais/demmon/internal/monitoring"
	"github.com/nm-morais/demmon/internal/monitoring/engine"
	monitoringProto "github.com/nm-morais/demmon/internal/monitoring/protocol"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
)

const (
	minProtosPort = 7000
	maxProtosPort = 8000

	minAnalyticsPort = 8000
	maxAnalyticsPort = 9000
)

var (
	protosPortVar     int
	analyticsPortVar  int
	randProtosPort    bool
	randAnalyticsPort bool
	cpuprofile        bool
	memprofile        bool
	silent            bool
	logFolder         string

	demmonTreeConf = &membershipProtocol.DemmonTreeConfig{
		LandmarkRedialTimer:           1 * time.Second,
		JoinMessageTimeout:            4 * time.Second,
		MaxTimeToProgressToNextLevel:  4 * time.Second,
		MaxRetriesJoinMsg:             3,
		Landmarks:                     nil,
		MinGrpSize:                    2,
		MaxGrpSize:                    9,
		NrPeersToKickPerParent:        3,
		NrPeersToBecomeParentInAbsorb: 3,
		PhiLevelForNodeDown:           3,
		SwitchProbability:             0.5,

		LimitFirstLevelGroupSize:      true,
		CheckChildenSizeTimerDuration: 3 * time.Second,
		ParentRefreshTickDuration:     3 * time.Second,
		ChildrenRefreshTickDuration:   3 * time.Second,
		RejoinTimerDuration:           10 * time.Second,

		MinLatencyImprovementToImprovePosition: 20 * time.Millisecond,
		AttemptImprovePositionProbability:      0.2,
		EvalMeasuredPeersRefreshTickDuration:   5 * time.Second,

		EnableSwitch: false,

		EmitWalkProbability:                0.33,
		BiasedWalkProbability:              0.2,
		BiasedWalkTTL:                      5,
		RandomWalkTTL:                      6,
		EmitWalkTimeout:                    8 * time.Second,
		MaxPeersInEView:                    15,
		MeasureNewPeersRefreshTickDuration: 7 * time.Second,
		MaxMeasuredPeers:                   5,
		NrHopsToIgnoreWalk:                 2,
		NrPeersInWalkMessage:               20,
		NrPeersToMeasureBiased:             2,
		NrPeersToMeasureRandom:             1,
		NrPeersToMergeInWalkSample:         5,

		CheckSwitchOportunityTimeout:          7500 * time.Millisecond,
		MinLatencyImprovementPerPeerForSwitch: 10 * time.Millisecond,
	}

	nodeWatcherConf = &pkg.NodeWatcherConf{
		PrintLatencyToInterval:    5 * time.Second,
		MaxRedials:                2,
		HbTickDuration:            1000 * time.Millisecond,
		NrMessagesWithoutWait:     3,
		NewLatencyWeight:          0.25,
		NrTestMessagesToSend:      1,
		NrTestMessagesToReceive:   1,
		OldLatencyWeight:          0.75,
		TcpTestTimeout:            3 * time.Second,
		UdpTestTimeout:            3 * time.Second,
		EvalConditionTickDuration: 1500 * time.Millisecond,
		MinSamplesLatencyEstimate: 3,

		WindowSize:             20,
		AcceptableHbPause:      1500 * time.Millisecond,
		FirstHeartbeatEstimate: 1500 * time.Millisecond,
		MinStdDeviation:        500 * time.Millisecond,
		PhiThreshold:           8.0,
	}
)

func main() {
	const (
		baseProtoPort     uint16 = 1200
		baseAnalyticsPort uint16 = 1300
	)

	ParseFlags()

	landmarks := []*membershipProtocol.PeerWithIDChain{
		membershipProtocol.NewPeerWithIDChain(
			membershipProtocol.PeerIDChain{membershipProtocol.PeerID{12}},
			peer.NewPeer(net.ParseIP("10.10.1.16"), baseProtoPort, baseAnalyticsPort),
			0,
			0,
			make(membershipProtocol.Coordinates, 4),
		),
		// membershipProtocol.NewPeerWithIdChain(
		// 	membershipProtocol.PeerIDChain{membershipProtocol.PeerID{17}},
		// 	peer.NewPeer(net.IPv4(10, 10, 50, 133),
		// 		baseProtoPort,
		// 		baseAnalyticsPort),
		// 	0,
		// 	0,
		// 	make(membershipProtocol.Coordinates, 4)),
		// membershipProtocol.NewPeerWithIdChain(
		// 	membershipProtocol.PeerIDChain{membershipProtocol.PeerID{23}},
		// 	peer.NewPeer(net.IPv4(10, 10, 29, 25),
		// 		baseProtoPort,
		// 		baseAnalyticsPort),
		// 	0,
		// 	0,
		// 	make(membershipProtocol.Coordinates, 4)),
		// membershipProtocol.NewPeerWithIdChain(
		// 	membershipProtocol.PeerIDChain{membershipProtocol.PeerID{23}},
		// 	peer.NewPeer(net.IPv4(10, 10, 1, 21),
		// 		baseProtoPort,
		// 		baseAnalyticsPort),
		// 	0,
		// 	0,
		// 	make(membershipProtocol.Coordinates, 4)),
	}
	demmonTreeConf.Landmarks = landmarks

	if logFolder == "" {
		logFolder = "/tmp/logs"
	}

	logFolder = fmt.Sprintf("%s/%s:%d", logFolder, GetLocalIP(), uint16(protosPortVar))

	babelConf := &pkg.Config{
		Silent:           silent,
		Cpuprofile:       cpuprofile,
		Memprofile:       memprofile,
		LogFolder:        logFolder,
		HandshakeTimeout: 3 * time.Second,
		Peer:             peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	exporterConfs := &exporter.Conf{
		Silent:          silent,
		LogFolder:       logFolder,
		ImporterHost:    "localhost",
		ImporterPort:    8090,
		LogFile:         "exporter.log",
		DialAttempts:    3,
		DialBackoffTime: 1 * time.Second,
		DialTimeout:     3 * time.Second,
		RequestTimeout:  3 * time.Second,
	}

	dConf := &monitoring.DemmonConf{
		Silent:     silent,
		LogFolder:  logFolder,
		LogFile:    "metrics_frontend.log",
		ListenPort: 8090,
		PluginDir:  fmt.Sprintf("%s/plugins", logFolder),
	}

	meConf := &engine.Conf{
		Silent:    silent,
		LogFolder: logFolder,
		LogFile:   "metrics_engine.log",
	}

	if randProtosPort {
		protosPortVar = int(getRandInt(int64(maxProtosPort-minProtosPort))) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = int(getRandInt(int64(maxAnalyticsPort-minAnalyticsPort))) + minAnalyticsPort
	}

	fmt.Println("Self peer: ", babelConf.Peer.String())
	start(babelConf, nodeWatcherConf, exporterConfs, dConf, demmonTreeConf, meConf)
	select {}
}

func start(
	babelConf *pkg.Config, nwConf *pkg.NodeWatcherConf, eConf *exporter.Conf,
	dConf *monitoring.DemmonConf, membershipConf *membershipProtocol.DemmonTreeConfig,
	meConf *engine.Conf,
) {
	babel := pkg.NewProtoManager(*babelConf)
	nw := pkg.NewNodeWatcher(
		*nwConf,
		babel,
	)
	babel.RegisterNodeWatcher(nw)
	babel.RegisterListenAddr(babelConf.Peer.ToTCPAddr())
	babel.RegisterListenAddr(babelConf.Peer.ToUDPAddr())

	db := tsdb.GetDB(logFolder, "tsdb.log", false, true)
	me := engine.NewMetricsEngine(db, *meConf, true)
	fm := membershipFrontend.New(babel)
	monitorProto := monitoringProto.New(babel, db, me)
	monitor := monitoring.New(*dConf, monitorProto, me, db, fm)

	babel.RegisterProtocol(monitorProto)
	babel.RegisterProtocol(membershipProtocol.New(*membershipConf, babel, nw))

	go monitor.Listen()
	go babel.Start()
	go setupDemmonExporter(eConf)
	go setupDemmonMetrics()
}

func GetLocalIP() net.IP {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(err)
	}

	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP
			}
		}
	}

	panic("no available loopback interfaces")
}

func ParseFlags() {
	flag.IntVar(&protosPortVar, "protos", 1200, "protos")
	flag.BoolVar(&silent, "s", false, "s")
	flag.StringVar(&logFolder, "l", "", "log file")
	flag.BoolVar(&randProtosPort, "rprotos", false, "port")
	flag.IntVar(&analyticsPortVar, "analytics", 1201, "analytics")
	flag.BoolVar(&randAnalyticsPort, "ranalytics", false, "port")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "cpuprofile")
	flag.BoolVar(&memprofile, "memprofile", false, "memprofile")
	flag.Parse()
}

func setupDemmonMetrics() {
	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 5 * time.Second
		defaultTTL         = 1
		defaultMetricCount = 1
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
	)

	const tickerTimeout = 5 * time.Second

	clientConf := client.DemmonClientConf{
		DemmonPort:     8090,
		DemmonHostAddr: "localhost",
		RequestTimeout: 1 * time.Second,
	}
	cl := client.New(clientConf)

	for i := 0; i < 3; i++ {
		err := cl.ConnectTimeout(connectTimeout)
		if err != nil {
			time.Sleep(connectBackoffTime)
			continue
		}

		break
	}

	_, err := cl.InstallContinuousQuery(
		`Max(Select("nr_goroutines","*"),"*")`,
		"the max of series nr_goroutines",
		expressionTimeout,
		exportFrequency,
		"nr_goroutines_max",
		defaultMetricCount,
		maxRetries,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallContinuousQuery(
		`Avg(Select("nr_goroutines","*"),"*")`,
		"the rolling average of series nr_goroutines",
		expressionTimeout,
		exportFrequency,
		"nr_goroutines_avg",
		defaultMetricCount,
		maxRetries,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallContinuousQuery(
		`Min(Select("nr_goroutines","*"),"*")`,
		"the min of series nr_goroutines",
		expressionTimeout,
		exportFrequency,
		"nr_goroutines_min",
		defaultMetricCount,
		maxRetries,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallNeighborhoodInterestSet(
		`SelectLast("nr_goroutines","*")`,
		expressionTimeout,
		defaultTTL,
		"nr_goroutines_neigh",
		exportFrequency,
		defaultMetricCount,
		maxRetries,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallContinuousQuery(
		`
		neighRoutines = Select("nr_goroutines_neigh","*")
		result = Min(neighRoutines, "*")
		`,
		"the min of nr_goroutines of the neighborhood",
		expressionTimeout,
		exportFrequency,
		"neigh_routines_min",
		defaultMetricCount,
		maxRetries,
	)

	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {
		res, err := cl.Query(
			"SelectLast('nr_goroutines_neigh',{'host':'.*'})",
			1*time.Second,
		)

		fmt.Println("SelectLast('nr_goroutines_neigh',{'host':'.*'}) Query results :")

		for _, ts := range res {
			fmt.Printf("%+v, %+v\n", ts, err)
		}

		res, err = cl.Query(
			"Select('neigh_routines_min','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('neigh_routines_min','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.Name, ts.Tags, ts.Points)
		}

		res, err = cl.Query(
			"Select('nr_goroutines','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('nr_goroutines','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.Name, ts.Tags, ts.Points)
		}
	}
}

func setupDemmonExporter(eConf *exporter.Conf) {
	e, err := exporter.New(eConf, GetLocalIP().String(), "demmon", nil)
	if err != nil {
		panic(err)
	}

	exportFrequncy := 5 * time.Second
	g := e.NewGauge("nr_goroutines", 12)
	setTicker := time.NewTicker(1 * time.Second)

	go e.ExportLoop(context.TODO(), exportFrequncy)

	for range setTicker.C {
		g.Set(float64(runtime.NumGoroutine()))
	}
}

func getRandInt(max int64) int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		panic(err)
	}

	return n.Int64()
}
