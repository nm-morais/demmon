package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	exporter "github.com/nm-morais/demmon-exporter"
	"github.com/nm-morais/demmon/core"
	membershipFrontend "github.com/nm-morais/demmon/core/membership/frontend"
	membershipProtocol "github.com/nm-morais/demmon/core/membership/protocol"
	"github.com/nm-morais/demmon/core/monitoring/engine"
	monitoringProto "github.com/nm-morais/demmon/core/monitoring/protocol"
	"github.com/nm-morais/demmon/core/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/sirupsen/logrus"
)

const (
	WaitForStartEnvVar            = "WAIT_FOR_START"
	LandmarksEnvVarName           = "LANDMARKS"
	AdvertiseListenAddrEnvVarName = "NODE_IP"
	BenchmarkMembershipEnvName    = "BENCKMARK_MEMBERSHIP"
	BenchmarkDemmonEnvName        = "BENCKMARK_METRICS"
	BenchmarkDemmonTypeEnvName    = "BENCKMARK_METRICS_TYPE"
	UseBandwidthEnvName           = "USE_BW"
	BandwidthScoreEnvName         = "BW_SCORE"

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
	bwScore           *int64
	useBW             *bool

	demmonTreeConf = &membershipProtocol.DemmonTreeConfig{
		LandmarkRedialTimer: 5 * time.Second,
		JoinMessageTimeout:  10 * time.Second,
		MaxRetriesJoinMsg:   3,
		Landmarks:           nil,

		MaxDiffForBWScore: 15,

		MinGrpSize: 2,
		MaxGrpSize: 6,

		MinLatencyImprovementToImprovePosition: 75 * time.Millisecond,
		MaxLatencyDowngrade:                    30 * time.Millisecond,
		UseBwScore:                             true,

		PhiLevelForNodeDown: 5,

		CheckChildenSizeTimerDuration: 10 * time.Second,
		ParentRefreshTickDuration:     1 * time.Second,
		ChildrenRefreshTickDuration:   5 * time.Second,
		RejoinTimerDuration:           10 * time.Second,

		AttemptImprovePositionProbability: 0.5,
		// EvalMeasuredPeersRefreshTickDuration: 7 * time.Second,

		EmitWalkProbability:                0.33,
		BiasedWalkProbability:              0.2,
		BiasedWalkTTL:                      5,
		RandomWalkTTL:                      6,
		EmitWalkTimeout:                    8 * time.Second,
		MaxPeersInEView:                    20,
		MeasureNewPeersRefreshTickDuration: 7 * time.Second,
		MaxMeasuredPeers:                   15,
		NrHopsToIgnoreWalk:                 2,
		NrPeersInWalkMessage:               15,
		NrPeersToMeasureBiased:             2,
		NrPeersToMeasureRandom:             1,
		NrPeersToMergeInWalkSample:         5,

		UnderpopulatedGroupTimerDuration: 12 * time.Second,
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
				make(membershipProtocol.Coordinates, 4), 0, 0,
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
			DialTimeout:       2 * time.Second,
		},
		Silent:           silent,
		LogFolder:        logFolder,
		HandshakeTimeout: 6 * time.Second,
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
		RequestTimeout:  5 * time.Second,
	}

	advertiseListenAddr, ok := GetAdvertiseListenAddrVar()
	if ok {
		fmt.Println("Got advertise listen addr from env var:", advertiseListenAddr)
		nodeWatcherConf.AdvertiseListenAddr = net.ParseIP(advertiseListenAddr)
		babelConf.Peer = peer.NewPeer(net.ParseIP(advertiseListenAddr), uint16(protosPortVar), uint16(analyticsPortVar))
	}

	logLevel := logrus.ErrorLevel
	dConf := &core.DemmonConf{
		Silent:      silent,
		LogFolder:   logFolder,
		LogFile:     "metrics_frontend.log",
		ListenPort:  8090,
		LoggerLevel: &logLevel,
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

	demmonTreeConf.UseBwScore = GetUseBWEnvVar()
	if demmonTreeConf.UseBwScore {
		demmonTreeConf.BandwidthScore = GetBWScore()
	}
	if *useBW {
		demmonTreeConf.BandwidthScore = int(*bwScore)
	}

	if randProtosPort {
		protosPortVar = int(getRandInt(int64(maxProtosPort-minProtosPort))) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = int(getRandInt(int64(maxAnalyticsPort-minAnalyticsPort))) + minAnalyticsPort
	}

	demmonTreeConf.UseBwScore = GetUseBWEnvVar() || *useBW
	demmonTreeConf.BandwidthScore = GetBWScore()

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
	dConf *core.DemmonConf, membershipConf *membershipProtocol.DemmonTreeConfig,
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
	monitorProto := monitoringProto.New(babel, db, me, isLandmark)
	monitor := core.New(*dConf, monitorProto, me, db, fm, babel)
	babel.RegisterProtocol(monitorProto)
	if !waitForStart {
		babel.StartAsync()
	}

	go monitor.Listen()
	if isBenchmarkDemmonMetrics {
		fmt.Println("Benchmarking demmon metrics protocol")
		benchmarkType, ok := GetDemmonBenchmarkTypeEnvVar()
		if !ok {
			panic("No benchmark type specified for demmon")
		}
		fmt.Println(benchmarkType)
		benchmarkDemmonMetrics(eConf, isLandmark, benchmarkType) // TODO CHANGE HERE BENCHMARK TYPE
	}
	ticker := time.NewTicker(5 * time.Minute)
	// timeStart := time.Now()
	for range ticker.C {
		// memProfile(logFolder, fmt.Sprintf("memProfile-%d_min", int(time.Since(timeStart).Minutes())))
		logrus.Printf("Number of goroutines: %d", runtime.NumGoroutine())
		runtime.Gosched()
	}

	// buf := make([]byte, 1<<20)
	// stacklen := runtime.Stack(buf, true)
	// log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])

	// pprof.StopCPUProfile()
	// memProfile(logFolder, "memprofile")
	// os.Exit(0)
}

func ParseFlags() {
	bwScore = flag.Int64("bandwidth", 0, "bandwidth score")
	useBW = flag.Bool("useBW", false, "if bandwidths are used")
	flag.IntVar(&protosPortVar, "protos", int(baseProtoPort), "protos")
	flag.BoolVar(&silent, "s", false, "s")
	flag.StringVar(&logFolder, "l", "", "log file")
	flag.BoolVar(&randProtosPort, "rprotos", false, "port")
	flag.IntVar(&analyticsPortVar, "analytics", int(baseAnalyticsPort), "analytics")
	flag.BoolVar(&randAnalyticsPort, "ranalytics", false, "port")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "cpuprofile")
	flag.BoolVar(&memprofile, "memprofile", false, "memprofile")
	flag.Parse()
}
