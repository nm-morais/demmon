package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	exporter "github.com/nm-morais/demmon-exporter"
	"github.com/nm-morais/demmon/internal"
	membershipFrontend "github.com/nm-morais/demmon/internal/membership/frontend"
	membershipProtocol "github.com/nm-morais/demmon/internal/membership/protocol"
	"github.com/nm-morais/demmon/internal/monitoring/engine"
	monitoringProto "github.com/nm-morais/demmon/internal/monitoring/protocol"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
)

const (
	WaitForStartEnvVar            = "WAIT_FOR_START"
	LandmarksEnvVarName           = "LANDMARKS"
	AdvertiseListenAddrEnvVarName = "NODE_IP"
	BenchmarkMembershipEnvName    = "BENCKMARK_MEMBERSHIP"
	BenchmarkDemmonEnvName        = "BENCKMARK_METRICS"

	minProtosPort = 7000
	maxProtosPort = 8000

	minAnalyticsPort = 8000
	maxAnalyticsPort = 9000

	baseProtoPort     uint16 = 1200
	baseAnalyticsPort uint16 = 1300
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
		LandmarkRedialTimer: 5 * time.Second,
		JoinMessageTimeout:  10 * time.Second,
		MaxRetriesJoinMsg:   3,
		Landmarks:           nil,

		MinGrpSize: 3,
		MaxGrpSize: 9,

		NrPeersToBecomeParentInAbsorb:            2,
		NrPeersToBecomeChildrenPerParentInAbsorb: 3,

		// NrPeersToKickPerParent:                 3,
		MinLatencyImprovementToImprovePosition: 30 * time.Millisecond,

		PhiLevelForNodeDown: 3,
		// SwitchProbability:             0.5,

		CheckChildenSizeTimerDuration: 10 * time.Second,
		ParentRefreshTickDuration:     5 * time.Second,
		ChildrenRefreshTickDuration:   5 * time.Second,
		RejoinTimerDuration:           10 * time.Second,

		AttemptImprovePositionProbability:    0.2,
		EvalMeasuredPeersRefreshTickDuration: 5 * time.Second,

		// EnableSwitch: false,

		EmitWalkProbability:                0.33,
		BiasedWalkProbability:              0.2,
		BiasedWalkTTL:                      5,
		RandomWalkTTL:                      6,
		EmitWalkTimeout:                    8 * time.Second,
		MaxPeersInEView:                    10,
		MeasureNewPeersRefreshTickDuration: 7 * time.Second,
		MaxMeasuredPeers:                   5,
		NrHopsToIgnoreWalk:                 2,
		NrPeersInWalkMessage:               15,
		NrPeersToMeasureBiased:             2,
		NrPeersToMeasureRandom:             1,
		NrPeersToMergeInWalkSample:         5,

		MinLatencyImprovementPerPeerForSwitch: 20 * time.Millisecond,

		UnderpopulatedGroupTimerDuration: 5 * time.Second,
		// CheckSwitchOportunityTimeout:          7500 * time.Millisecond,
	}
)

func main() {
	ParseFlags()
	landmarks, ok := GetLandmarksEnv()
	benchmarkDemmonMetrics := GetBenchmarkDemmonEnvVar()
	benchmarkMembership := GetBenchmarkMembershipEnvVar()
	if !ok {
		landmarks = []*membershipProtocol.PeerWithIDChain{
			membershipProtocol.NewPeerWithIDChain(
				membershipProtocol.PeerIDChain{membershipProtocol.PeerID{12}},
				peer.NewPeer(net.ParseIP("10.10.1.16"), baseProtoPort, baseAnalyticsPort),
				0,
				0,
				make(membershipProtocol.Coordinates, 4),
			),
			// membershipProtocol.NewPeerWithIDChain(
			// 	membershipProtocol.PeerIDChain{membershipProtocol.PeerID{17}},
			// 	peer.NewPeer(net.IPv4(10, 10, 50, 133),
			// 		baseProtoPort,
			// 		baseAnalyticsPort),
			// 	0,
			// 	0,
			// 	make(membershipProtocol.Coordinates, 4)),
			// membershipProtocol.NewPeerWithIDChain(
			// 	membershipProtocol.PeerIDChain{membershipProtocol.PeerID{23}},
			// 	peer.NewPeer(net.IPv4(10, 10, 29, 25),
			// 		baseProtoPort,
			// 		baseAnalyticsPort),
			// 	0,
			// 	0,
			// 	make(membershipProtocol.Coordinates, 4)),
			// membershipProtocol.NewPeerWithIDChain(
			// 	membershipProtocol.PeerIDChain{membershipProtocol.PeerID{23}},
			// 	peer.NewPeer(net.IPv4(10, 10, 1, 21),
			// 		baseProtoPort,
			// 		baseAnalyticsPort),
			// 	0,
			// 	0,
			// 	make(membershipProtocol.Coordinates, 4)),
		}
	} else {
		fmt.Printf("Got landmarks from env var: %+v\n", landmarks)
	}

	demmonTreeConf.Landmarks = landmarks

	if logFolder == "" {
		logFolder = "/tmp/logs"
	}

	logFolder = fmt.Sprintf("%s/%s:%d", logFolder, GetLocalIP(), uint16(protosPortVar))
	_ = os.MkdirAll(logFolder, os.ModePerm)

	// f, err := os.Create(fmt.Sprintf("%s/%s", logFolder, "cpuProfile"))
	// if err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }
	// defer f.Close() // error handling omitted for example
	// if err := pprof.StartCPUProfile(f); err != nil {
	// 	log.Fatal("could not start CPU profile: ", err)
	// }

	nodeWatcherConf := &pkg.NodeWatcherConf{
		PrintLatencyToInterval:    10 * time.Second,
		EvalConditionTickDuration: 1500 * time.Millisecond,
		MaxRedials:                2,
		TcpTestTimeout:            10 * time.Second,
		UdpTestTimeout:            10 * time.Second,
		NrTestMessagesToSend:      1,
		NrMessagesWithoutWait:     3,
		NrTestMessagesToReceive:   1,
		HbTickDuration:            1000 * time.Millisecond,
		MinSamplesLatencyEstimate: 3,
		OldLatencyWeight:          0.75,
		NewLatencyWeight:          0.25,
		PhiThreshold:              8.0,
		WindowSize:                20,
		MinStdDeviation:           500 * time.Millisecond,
		AcceptableHbPause:         1500 * time.Millisecond,
		FirstHeartbeatEstimate:    1500 * time.Millisecond,

		AdvertiseListenAddr: nil,
		ListenAddr:          GetLocalIP(),
		ListenPort:          1300,
	}

	babelConf := &pkg.Config{
		SmConf: pkg.StreamManagerConf{
			BatchMaxSizeBytes: 1500,
			BatchTimeout:      500 * time.Millisecond,
			DialTimeout:       10 * time.Second,
		},
		Silent:           silent,
		LogFolder:        logFolder,
		HandshakeTimeout: 20 * time.Second,
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
		DialTimeout:     5 * time.Second,
		RequestTimeout:  3 * time.Second,
	}

	advertiseListenAddr, ok := GetAdvertiseListenAddrVar()
	if ok {
		fmt.Println("Got advertise listen addr from env var:", advertiseListenAddr)
		nodeWatcherConf.AdvertiseListenAddr = net.ParseIP(advertiseListenAddr)
		babelConf.Peer = peer.NewPeer(net.ParseIP(advertiseListenAddr), uint16(protosPortVar), uint16(analyticsPortVar))
	}

	dConf := &internal.DemmonConf{
		Silent:     silent,
		LogFolder:  logFolder,
		LogFile:    "metrics_frontend.log",
		ListenPort: 8090,
	}

	meConf := &engine.Conf{
		Silent:    silent,
		LogFolder: logFolder,
		LogFile:   "metrics_engine.log",
	}

	dbConf := &tsdb.Conf{
		SetupLogToFile:   true,
		Silent:           silent,
		LogFolder:        logFolder,
		LogFile:          "tsdb.log",
		CleanupFrequency: 5 * time.Second,
	}

	if randProtosPort {
		protosPortVar = int(getRandInt(int64(maxProtosPort-minProtosPort))) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = int(getRandInt(int64(maxAnalyticsPort-minAnalyticsPort))) + minAnalyticsPort
	}

	fmt.Println("Self peer: ", babelConf.Peer.String())
	isLandmark := false
	for _, p := range landmarks {
		if peer.PeersEqual(babelConf.Peer, p) {
			isLandmark = true
			break
		}
	}
	waitForStart := GetWaitForStartEnvVar()
	start(babelConf,
		nodeWatcherConf,
		exporterConfs,
		dConf,
		demmonTreeConf,
		meConf,
		dbConf,
		isLandmark,
		waitForStart,
		benchmarkDemmonMetrics,
		benchmarkMembership)
}

func start(
	babelConf *pkg.Config, nwConf *pkg.NodeWatcherConf, eConf *exporter.Conf,
	dConf *internal.DemmonConf, membershipConf *membershipProtocol.DemmonTreeConfig,
	meConf *engine.Conf, dbConf *tsdb.Conf, isLandmark, waitForStart, isBenchmarkDemmonMetrics, benchmarkMembership bool,
) {

	babel := pkg.NewProtoManager(*babelConf)
	nw := pkg.NewNodeWatcher(
		*nwConf,
		babel,
	)

	babel.RegisterNodeWatcher(nw)
	babel.RegisterListenAddr(&net.TCPAddr{
		IP:   GetLocalIP(),
		Port: protosPortVar,
	})
	babel.RegisterListenAddr(&net.UDPAddr{
		IP:   GetLocalIP(),
		Port: protosPortVar,
	})

	fm := membershipFrontend.New(babel)
	babel.RegisterProtocol(membershipProtocol.New(membershipConf, babel, nw))
	if benchmarkMembership {
		fmt.Println("Benchmarking demmon membership protocol")
		babel.StartSync()
	}

	db := tsdb.GetDB(dbConf)
	me := engine.NewMetricsEngine(db, *meConf, true)
	monitorProto := monitoringProto.New(babel, db, me)
	monitor := internal.New(*dConf, monitorProto, me, db, fm, babel)
	babel.RegisterProtocol(monitorProto)
	if !waitForStart {
		babel.StartAsync()
	}

	go monitor.Listen()
	if isBenchmarkDemmonMetrics {
		fmt.Println("Benchmarking demmon metrics protocol")
		benchmarkDemmonMetrics(eConf, isLandmark)
	}
	select {}

	// buf := make([]byte, 1<<20)
	// stacklen := runtime.Stack(buf, true)
	// log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])

	// pprof.StopCPUProfile()
	// memProfile(logFolder, "memprofile")
	// os.Exit(0)
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
