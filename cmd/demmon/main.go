package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	client "github.com/nm-morais/demmon-client/pkg"
	"github.com/nm-morais/demmon-common/body_types"
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
	LandmarksEnvVarName           = "LANDMARKS"
	AdvertiseListenAddrEnvVarName = "NODE_IP"
	minProtosPort                 = 7000
	maxProtosPort                 = 8000

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
		LandmarkRedialTimer:           1 * time.Second,
		JoinMessageTimeout:            4 * time.Second,
		MaxTimeToProgressToNextLevel:  4 * time.Second,
		MaxRetriesJoinMsg:             3,
		Landmarks:                     nil,
		MinGrpSize:                    2,
		MaxGrpSize:                    5,
		NrPeersToKickPerParent:        2,
		NrPeersToBecomeParentInAbsorb: 2,
		PhiLevelForNodeDown:           3,
		// SwitchProbability:             0.5,

		LimitFirstLevelGroupSize:      true,
		CheckChildenSizeTimerDuration: 3 * time.Second,
		ParentRefreshTickDuration:     3 * time.Second,
		ChildrenRefreshTickDuration:   3 * time.Second,
		RejoinTimerDuration:           10 * time.Second,

		MinLatencyImprovementToImprovePosition: 25 * time.Millisecond,
		AttemptImprovePositionProbability:      0.2,
		EvalMeasuredPeersRefreshTickDuration:   5 * time.Second,

		// EnableSwitch: false,

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

		// CheckSwitchOportunityTimeout:          7500 * time.Millisecond,
		MinLatencyImprovementPerPeerForSwitch: 25 * time.Millisecond,
	}
)

func main() {
	ParseFlags()

	landmarks, ok := GetLandmarksEnv()

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
	}

	demmonTreeConf.Landmarks = landmarks

	if logFolder == "" {
		logFolder = "/tmp/logs"
	}

	logFolder = fmt.Sprintf("%s/%s:%d", logFolder, GetLocalIP(), uint16(protosPortVar))

	nodeWatcherConf := &pkg.NodeWatcherConf{
		PrintLatencyToInterval:    5 * time.Second,
		EvalConditionTickDuration: 1500 * time.Millisecond,
		MaxRedials:                2,
		TcpTestTimeout:            3 * time.Second,
		UdpTestTimeout:            3 * time.Second,
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
		AdvertiseListenPort: 1300,
		ListenAddr:          GetLocalIP(),
		ListenPort:          1300,
	}

	babelConf := &pkg.Config{
		Silent:           silent,
		Cpuprofile:       cpuprofile,
		Memprofile:       memprofile,
		LogFolder:        logFolder,
		HandshakeTimeout: 3 * time.Second,
		Peer:             peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	advertiseListenAddr, ok := GetAdvertiseListenAddrVar()
	if ok {
		nodeWatcherConf.AdvertiseListenAddr = net.ParseIP(advertiseListenAddr)
		babelConf.Peer = peer.NewPeer(net.ParseIP(advertiseListenAddr), uint16(protosPortVar), uint16(analyticsPortVar))
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
		SetupLogToFile: true,
		Silent:         silent,
		LogFolder:      logFolder,
		LogFile:        "tsdb.log",
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
	start(babelConf, nodeWatcherConf, exporterConfs, dConf, demmonTreeConf, meConf, dbConf, isLandmark)
}

func start(
	babelConf *pkg.Config, nwConf *pkg.NodeWatcherConf, eConf *exporter.Conf,
	dConf *internal.DemmonConf, membershipConf *membershipProtocol.DemmonTreeConfig,
	meConf *engine.Conf, dbConf *tsdb.Conf, isLandmark bool,
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

	fmt.Printf("Starting db with conf: %+v\n", dbConf)

	db := tsdb.GetDB(dbConf)
	me := engine.NewMetricsEngine(db, *meConf, true)
	fm := membershipFrontend.New(babel)
	monitorProto := monitoringProto.New(babel, db, me)
	monitor := internal.New(*dConf, monitorProto, me, db, fm)

	babel.RegisterProtocol(monitorProto)
	babel.RegisterProtocol(membershipProtocol.New(membershipConf, babel, nw))

	go monitor.Listen()
	go testDemmonMetrics(eConf, isLandmark)
	babel.StartSync()
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

func GetAdvertiseListenAddrVar() (string, bool) {
	hostIP, ok := os.LookupEnv(AdvertiseListenAddrEnvVarName)
	if !ok {
		return "", false
	}
	return hostIP, true
}

func GetLandmarksEnv() ([]*membershipProtocol.PeerWithIDChain, bool) {
	landmarksEnv, exists := os.LookupEnv(LandmarksEnvVarName)
	if !exists {
		return nil, false
	}

	landmarksSplitted := strings.Split(landmarksEnv, ";")
	landmarks := make([]*membershipProtocol.PeerWithIDChain, len(landmarksSplitted))

	for i, landmarkIP := range landmarksSplitted {
		landmark := membershipProtocol.NewPeerWithIDChain(
			membershipProtocol.PeerIDChain{membershipProtocol.PeerID{uint8(i)}},
			peer.NewPeer(net.ParseIP(landmarkIP), baseProtoPort, baseAnalyticsPort),
			0,
			0,
			make(membershipProtocol.Coordinates, 4),
		)
		landmarks[i] = landmark
	}
	return landmarks, true
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

func testDemmonMetrics(eConf *exporter.Conf, isLandmark bool) {
	go exportMetrics(eConf)
	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 5 * time.Second
		defaultTTL         = 2
		defaultMetricCount = 5
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 5 * time.Second
		requestTimeout     = 1 * time.Second
	)

	clientConf := client.DemmonClientConf{
		DemmonPort:     8090,
		DemmonHostAddr: "localhost",
		RequestTimeout: requestTimeout,
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
	// testCustomInterestSet(cl)
	if isLandmark {
		testGlobalAggFunc(cl)
	}
}

func exportMetrics(eConf *exporter.Conf) {
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

func testNeighInterestSets(cl *client.DemmonClient) {
	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 5 * time.Second
		defaultTTL         = 2
		defaultMetricCount = 12
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 5 * time.Second
		requestTimeout     = 1 * time.Second
	)

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

	_, err = cl.InstallNeighborhoodInterestSet(&body_types.NeighborhoodInterestSet{
		IS: body_types.InterestSet{
			MaxRetries: maxRetries,
			Query: body_types.RunnableExpression{
				Expression: `SelectLast("nr_goroutines","*")`,
				Timeout:    expressionTimeout,
			},
			OutputBucketOpts: body_types.BucketOptions{
				Name: "nr_goroutines_neigh",
				Granularity: body_types.Granularity{
					Granularity: exportFrequency,
					Count:       3,
				},
			},
		},
		TTL: defaultTTL,
	})

	if err != nil {
		panic(err)
	}

	_, err = cl.InstallContinuousQuery(
		`neighRoutines = Select("nr_goroutines_neigh","*")
		 result = Min(neighRoutines, "*")`,
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
			"Select('nr_goroutines_neigh','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('nr_goroutines_neigh','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
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
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}

		res, err = cl.Query(
			"Select('nr_goroutines_avg','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('nr_goroutines_avg','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}
	}
}

func testTreeAggFunc(cl *client.DemmonClient) {

	// 	`neighRoutines = Select("nr_goroutines_neigh","*")
	// 	result = Min(neighRoutines, "*")`,
	//    "the min of nr_goroutines of the neighborhood",
	//    expressionTimeout,
	//    exportFrequency,
	//    "neigh_routines_min",
	//    defaultMetricCount,
	//    maxRetries,

	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 5 * time.Second
		defaultTTL         = 2
		defaultMetricCount = 12
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 5 * time.Second
		requestTimeout     = 1 * time.Second
	)

	_, err := cl.InstallTreeAggregationFunction(
		&body_types.TreeAggregationSet{
			MaxRetries: 3,
			Query: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `point = SelectLast("nr_goroutines","*")[0].Last()
							result = {"count":1, "value":point.Value().value}`,
			},
			OutputBucketOpts: body_types.BucketOptions{
				Name: "avg_nr_goroutines_tree",
				Granularity: body_types.Granularity{
					Granularity: exportFrequency,
					Count:       defaultMetricCount,
				},
			},
			MergeFunction: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `
							aux = {"count":0, "value":0}
							for (i = 0; i < args.length; i++) {
								aux.count += args[i].count
								aux.value += args[i].value					
							}
							result = aux
							`,
			},
			Levels: 10,
		})

	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {
		res, err := cl.Query(
			"Select('avg_nr_goroutines_tree','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('avg_nr_goroutines_tree','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}
	}
}

func testGlobalAggFunc(cl *client.DemmonClient) {

	const (
		connectBackoffTime = 1 * time.Second
		expressionTimeout  = 1 * time.Second
		exportFrequency    = 5 * time.Second
		defaultTTL         = 2
		defaultMetricCount = 12
		maxRetries         = 3
		connectTimeout     = 3 * time.Second
		tickerTimeout      = 5 * time.Second
		requestTimeout     = 1 * time.Second
	)

	_, err := cl.InstallGlobalAggregationFunction(
		&body_types.GlobalAggregationFunction{
			MaxRetries: 3,
			Query: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `point = SelectLast("nr_goroutines","*")[0].Last()
							result = {"count":1, "value":point.Value().value}`,
			},
			OutputBucketOpts: body_types.BucketOptions{
				Name: "avg_nr_goroutines_global",
				Granularity: body_types.Granularity{
					Granularity: exportFrequency,
					Count:       defaultMetricCount,
				},
			},
			MergeFunction: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `
							aux = {"count":0, "value":0}
							for (i = 0; i < args.length; i++) {
								aux.count += args[i].count
								aux.value += args[i].value					
							}
							result = aux
							`,
			},
			DifferenceFunction: body_types.RunnableExpression{
				Timeout: expressionTimeout,
				Expression: `
							toSubtractFrom = args[0]
							for (i = 1; i < args.length; i++) {
								toSubtractFrom.count -= args[i].count
								toSubtractFrom.value -= args[i].value					
							}
							result = toSubtractFrom
							`,
			},
		})

	if err != nil {
		panic(err)
	}

	for range time.NewTicker(tickerTimeout).C {
		res, err := cl.Query(
			"Select('avg_nr_goroutines_global','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('avg_nr_goroutines_global','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}
	}
}

func testNodeUpdates(cl *client.DemmonClient) {

	res, err, _, updateChan := cl.SubscribeNodeUpdates()

	if err != nil {
		panic(err)
	}

	fmt.Println("Starting view:", res)

	go func() {
		for nodeUpdate := range updateChan {
			fmt.Printf("Node Update: %+v\n", nodeUpdate)
		}
	}()
}

func testMsgBroadcast(cl *client.DemmonClient) {
	const tickerTimeout = 5 * time.Second

	msgChan, _, err := cl.InstallBroadcastMessageHandler(1)

	if err != nil {
		panic(err)
	}

	go func() {
		for range time.NewTicker(tickerTimeout).C {
			err := cl.BroadcastMessage(
				body_types.Message{
					ID:      1,
					TTL:     2,
					Content: struct{ Message string }{Message: "hey"}},
			)
			if err != nil {
				panic(err)
			}
		}
	}()

	go func() {
		for msg := range msgChan {
			fmt.Printf("Got message:%+v\n", msg)
		}
	}()
}

func testCustomInterestSet(cl *client.DemmonClient) {
	const (
		tickerTimeout     = 5 * time.Second
		exportFrequency   = 5 * time.Second
		expressionTimeout = 1 * time.Second
	)
	_, errChan, _, err := cl.InstallCustomInterestSet(body_types.CustomInterestSet{
		DialTimeout:      3 * time.Second,
		DialRetryBackoff: 5 * time.Second,
		Hosts: []body_types.CustomInterestSetHost{
			{
				IP:   demmonTreeConf.Landmarks[0].Peer.IP(),
				Port: 8090,
			},
		},
		IS: body_types.InterestSet{MaxRetries: 3,
			Query: body_types.RunnableExpression{
				Timeout:    expressionTimeout,
				Expression: `SelectLast("nr_goroutines","*")`,
			},
			OutputBucketOpts: body_types.BucketOptions{
				Name: "nr_goroutines_landmarks",
				Granularity: body_types.Granularity{
					Granularity: exportFrequency,
					Count:       10,
				},
			},
		},
	})

	if err != nil {
		panic(err)
	}

	go func() {
		for err := range errChan {
			panic(err)
		}
	}()

	for range time.NewTicker(tickerTimeout).C {

		res, err := cl.Query(
			"Select('nr_goroutines_landmarks','*')",
			1*time.Second,
		)
		if err != nil {
			panic(err)
		}

		fmt.Println("Select('nr_goroutines_landmarks','*') Query results :")

		for idx, ts := range res {
			fmt.Printf("%d) %s:%+v:%+v\n", idx, ts.MeasurementName, ts.TSTags, ts.Values)
		}
	}
}
