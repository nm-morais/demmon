package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"time"

	client "github.com/nm-morais/demmon-client/pkg"
	exporter "github.com/nm-morais/demmon-exporter"
	"github.com/nm-morais/demmon/internal/membership/membership_frontend"
	"github.com/nm-morais/demmon/internal/membership/membership_protocol"
	"github.com/nm-morais/demmon/internal/monitoring"
	"github.com/nm-morais/demmon/internal/monitoring/metrics_engine"
	"github.com/nm-morais/demmon/internal/monitoring/protocol/monitoring_proto"
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
	demmonTreeConf = membership_protocol.DemmonTreeConfig{
		LandmarkRedialTimer:           1 * time.Second,
		JoinMessageTimeout:            4 * time.Second,
		MaxTimeToProgressToNextLevel:  4 * time.Second,
		MaxRetriesJoinMsg:             3,
		Landmarks:                     landmarks,
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

	nodeWatcherConf = pkg.NodeWatcherConf{
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

	landmarks = []*membership_protocol.PeerWithIdChain{
		membership_protocol.NewPeerWithIdChain(membership_protocol.PeerIDChain{membership_protocol.PeerID{12}}, peer.NewPeer(net.IPv4(10, 10, 1, 16), 1200, 1300), 0, 0, make(membership_protocol.Coordinates, 4)),
		// membership_protocol.NewPeerWithIdChain(membership_protocol.PeerIDChain{membership_protocol.PeerID{17}}, peer.NewPeer(net.IPv4(10, 10, 50, 133), 1200, 1300), 0, 0, make(membership_protocol.Coordinates, 4)),
		// membership_protocol.NewPeerWithIdChain(membership_protocol.PeerIDChain{membership_protocol.PeerID{23}}, peer.NewPeer(net.IPv4(10, 10, 29, 25), 1200, 1300), 0, 0, make(membership_protocol.Coordinates, 4)),
		// membership_protocol.NewPeerWithIdChain(membership_protocol.PeerIDChain{membership_protocol.PeerID{23}}, peer.NewPeer(net.IPv4(10, 10, 1, 21), 1200, 1300), 0, 0, make(membership_protocol.Coordinates, 4)),
	}

	protosPortVar     int
	analyticsPortVar  int
	randProtosPort    bool
	randAnalyticsPort bool
	cpuprofile        bool
	memprofile        bool
	silent            bool
	logFolder         string
)

func main() {

	rand.Seed(time.Now().Unix() + rand.Int63())

	flag.IntVar(&protosPortVar, "protos", 1200, "protos")
	flag.BoolVar(&silent, "s", false, "s")

	flag.StringVar(&logFolder, "l", "", "log file")

	flag.BoolVar(&randProtosPort, "rprotos", false, "port")
	flag.IntVar(&analyticsPortVar, "analytics", 1201, "analytics")
	flag.BoolVar(&randAnalyticsPort, "ranalytics", false, "port")

	flag.BoolVar(&cpuprofile, "cpuprofile", false, "cpuprofile")
	flag.BoolVar(&memprofile, "memprofile", false, "memprofile")

	flag.Parse()

	if logFolder == "" {
		logFolder = "/tmp/logs"
	}

	logFolder = fmt.Sprintf("%s/%s:%d", logFolder, GetLocalIP(), uint16(protosPortVar))

	babelConf := pkg.Config{
		Silent:           silent,
		Cpuprofile:       cpuprofile,
		Memprofile:       memprofile,
		LogFolder:        logFolder,
		HandshakeTimeout: 3 * time.Second,
		Peer:             peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	exporterConfs := exporter.Conf{
		Silent:          silent,
		LogFolder:       logFolder,
		ImporterHost:    "localhost",
		ImporterPort:    8090,
		LogFile:         "exporter.log",
		DialAttempts:    3,
		DialBackoffTime: 1 * time.Second,
		DialTimeout:     3 * time.Second,
		RequestTimeout:  1 * time.Second,
	}

	dConf := monitoring.DemmonConf{
		Silent:     silent,
		LogFolder:  logFolder,
		LogFile:    "demmon_frontend.log",
		ListenPort: 8090,
		PluginDir:  fmt.Sprintf("%s/plugins", logFolder),
	}

	if randProtosPort {
		protosPortVar = rand.Intn(maxProtosPort-minProtosPort) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = rand.Intn(maxAnalyticsPort-minAnalyticsPort) + minAnalyticsPort
	}

	fmt.Println("Self peer: ", babelConf.Peer.String())
	err := start(babelConf, nodeWatcherConf, exporterConfs, dConf, demmonTreeConf)
	if err != nil {
		panic(err)
	}

	select {}
}

func start(babelConf pkg.Config, nwConf pkg.NodeWatcherConf, eConf exporter.Conf, dConf monitoring.DemmonConf, membershipConf membership_protocol.DemmonTreeConfig) error {
	babel := pkg.NewProtoManager(babelConf)
	nw := pkg.NewNodeWatcher(nwConf, babel)
	babel.RegisterNodeWatcher(nw)
	babel.RegisterListenAddr(babelConf.Peer.ToTCPAddr())
	babel.RegisterListenAddr(babelConf.Peer.ToUDPAddr())

	db := tsdb.GetDB()
	me := metrics_engine.NewMetricsEngine(db)
	fm := membership_frontend.New(babel)
	monitorProto := monitoring_proto.New(babel, db, me)
	monitor := monitoring.New(dConf, babel, monitorProto, me, db, fm)

	babel.RegisterProtocol(monitorProto)
	babel.RegisterProtocol(membership_protocol.New(membershipConf, babel, nw))

	go monitor.Listen()
	go babel.Start()
	go setupDemmonExporter(eConf)
	go setupDemmonMetrics()
	return nil
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

func setupDemmonMetrics() {
	clientConf := client.DemmonClientConf{
		DemmonPort:     8090,
		DemmonHostAddr: "localhost",
		RequestTimeout: 1 * time.Second,
	}
	cl := client.New(clientConf)
	for i := 0; i < 3; i++ {
		err := cl.ConnectTimeout(3 * time.Second)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	_, err := cl.InstallContinuousQuery(
		`
		var goroutines_max = Max(Select("nr_goroutines","*"),"*").Last()
		AddPoint("nr_goroutines_max", {}, goroutines_max)`,
		"the max of series nr_goroutines",
		1*time.Second,
		5*time.Second,
		"nr_goroutines_max",
		60,
		5,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallContinuousQuery(
		`
		var goroutines_avg = Avg(Select("nr_goroutines","*"),"*").Last()
		AddPoint("nr_goroutines_avg", {}, goroutines_avg)`,
		"the max of series nr_goroutines",
		1*time.Second,
		5*time.Second,
		"nr_goroutines_avg",
		60,
		5,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallContinuousQuery(
		`
		var goroutines_min = Min(Select("nr_goroutines","*"),"*").Last()
		AddPoint("nr_goroutines_min", {}, goroutines_min)`,
		"the min of series nr_goroutines",
		1*time.Second,
		5*time.Second,
		"nr_goroutines_min",
		60,
		5,
	)

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallNeighborhoodInterestSet(
		`SelectLast("nr_goroutines","*")`,
		1*time.Second,
		1,
		"nr_goroutines_neigh",
		3*time.Second,
		20,
		3,
	)

	if err != nil {
		panic(err)
	}

	for range time.NewTicker(5 * time.Second).C {
		// res, err := cl.Query(
		// 	`Select("nr_goroutines","*")`,
		// 	1*time.Second,
		// )
		// fmt.Println("nr_goroutines\n", res, err)

		// res, err := cl.Query(
		// 	`Select("nr_goroutines_max","*")`,
		// 	1*time.Second,
		// )
		// fmt.Printf("\nnr_goroutines_max %+v, %+v\n\n\n", res, err)

		// res, err = cl.Query(
		// 	`Select("nr_goroutines_avg","*")`,
		// 	1*time.Second,
		// )
		// fmt.Printf("\nnr_goroutines_avg %+v, %+v\n\n\n", res, err)

		res, err := cl.Query(
			`Select("nr_goroutines_neigh",{"host":".*"})`,
			1*time.Second,
		)
		fmt.Printf("\nnr_goroutines_neigh %+v, %+v\n", res, err)

		// activeQueries, err := cl.GetContinuousQueries()
		// fmt.Printf("\ncontinuous queries: %+v, %+v\n\n\n", activeQueries, err)
	}
}

func setupDemmonExporter(eConf exporter.Conf) {
	e, err := exporter.New(eConf, GetLocalIP().String(), "demmon", nil)
	if err != nil {
		panic(err)
	}

	g := e.NewGauge("nr_goroutines")
	exportTicker := time.NewTicker(5 * time.Second)
	go e.ExportLoop(context.TODO(), exportTicker.C)

	setTicker := time.NewTicker(1 * time.Second)
	for range setTicker.C {
		g.Set(float64(runtime.NumGoroutine()))
	}
}
