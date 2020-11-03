package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/nm-morais/demmon-common/default_plugin"
	"github.com/nm-morais/demmon-common/timeseries"
	exporter "github.com/nm-morais/demmon-exporter"
	"github.com/nm-morais/demmon/internal/membership/membership_protocol"
	"github.com/nm-morais/demmon/internal/monitoring"
	"github.com/nm-morais/demmon/internal/monitoring/metrics_manager"
	"github.com/nm-morais/demmon/internal/monitoring/plugin_manager"
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
		MeasuredPeersSize:                  5,
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
		membership_protocol.NewPeerWithIdChain(membership_protocol.PeerIDChain{membership_protocol.PeerID{12}}, peer.NewPeer(net.IPv4(10, 10, 211, 91), 1200, 1300), 0, 0, make(membership_protocol.Coordinates, 4)),
		membership_protocol.NewPeerWithIdChain(membership_protocol.PeerIDChain{membership_protocol.PeerID{17}}, peer.NewPeer(net.IPv4(10, 10, 50, 133), 1200, 1300), 0, 0, make(membership_protocol.Coordinates, 4)),
		membership_protocol.NewPeerWithIdChain(membership_protocol.PeerIDChain{membership_protocol.PeerID{23}}, peer.NewPeer(net.IPv4(10, 10, 29, 25), 1200, 1300), 0, 0, make(membership_protocol.Coordinates, 4)),
		membership_protocol.NewPeerWithIdChain(membership_protocol.PeerIDChain{membership_protocol.PeerID{23}}, peer.NewPeer(net.IPv4(10, 10, 1, 21), 1200, 1300), 0, 0, make(membership_protocol.Coordinates, 4)),
	}

	protosPortVar     int
	analyticsPortVar  int
	randProtosPort    bool
	randAnalyticsPort bool
	cpuprofile        bool
	memprofile        bool
	silent            bool
)

func main() {

	rand.Seed(time.Now().Unix() + rand.Int63())

	flag.IntVar(&protosPortVar, "protos", 1200, "protos")
	flag.BoolVar(&silent, "s", false, "s")

	flag.BoolVar(&randProtosPort, "rprotos", false, "port")
	flag.IntVar(&analyticsPortVar, "analytics", 1201, "analytics")
	flag.BoolVar(&randAnalyticsPort, "ranalytics", false, "port")

	flag.BoolVar(&cpuprofile, "cpuprofile", false, "cpuprofile")
	flag.BoolVar(&memprofile, "memprofile", false, "memprofile")

	flag.Parse()

	babelConf := pkg.Config{
		LogStdout:        !silent,
		Cpuprofile:       cpuprofile,
		Memprofile:       memprofile,
		LogFolder:        "/code/logs/",
		HandshakeTimeout: 3 * time.Second,
		Peer:             peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	LogFolder := fmt.Sprintf("/code/logs/%s:%d/", GetLocalIP(), uint16(protosPortVar))

	exporterConfs := exporter.Conf{
		LogFolder:                  LogFolder,
		ExportFrequency:            3 * time.Second,
		ImporterHost:               "localhost",
		ImporterPort:               8090,
		LogFile:                    "exporter.log",
		HTTPRequestTimeout:         1 * time.Second,
		MaxRegisterMetricsRetries:  5,
		RegisterMetricsBackoffTime: 3 * time.Second,
	}

	dConf := monitoring.DemmonConf{
		ListenPort: 8090,
	}

	pmconf := plugin_manager.PluginManagerConfig{
		WorkingDir: fmt.Sprintf("/tmp/logs/%s:%d/plugins", GetLocalIP(), uint16(protosPortVar)),
		LogFolder:  LogFolder,
		LogFile:    "plugin_manager.log",
	}

	mmConf := metrics_manager.MetricsManagerConf{
		LogFolder: LogFolder,
		LogFile:   "metrics_manager.log",
	}

	if randProtosPort {
		protosPortVar = rand.Intn(maxProtosPort-minProtosPort) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = rand.Intn(maxAnalyticsPort-minAnalyticsPort) + minAnalyticsPort
	}

	fmt.Println("Self peer: ", babelConf.Peer.String())
	err := start(babelConf, nodeWatcherConf, exporterConfs, dConf, pmconf, mmConf, demmonTreeConf)
	if err != nil {
		panic(err)
	}

	select {}
}

func start(babelConf pkg.Config, nwConf pkg.NodeWatcherConf, eConf exporter.Conf, dConf monitoring.DemmonConf, pmConf plugin_manager.PluginManagerConfig, mmConf metrics_manager.MetricsManagerConf, membershipConf membership_protocol.DemmonTreeConfig) error {
	babel := pkg.NewProtoManager(babelConf)
	e := exporter.New(eConf, babel)
	nw := pkg.NewNodeWatcher(nwConf, babel)
	babel.RegisterNodeWatcher(nw)
	babel.RegisterListenAddr(babelConf.Peer.ToTCPAddr())
	babel.RegisterListenAddr(babelConf.Peer.ToUDPAddr())
	babel.RegisterProtocol(membership_protocol.New(membershipConf, babel, nw))
	monitor := monitoring.New(dConf, pmConf, mmConf)
	go monitor.Start()
	go babel.Start()
	go setupDemmonMetrics(e)
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

func setupDemmonMetrics(e *exporter.Exporter) {
	g := default_plugin.NewFloatGauge("system", "goroutine_count", 0)
	e.RegisterMetric(g, timeseries.DefaultGranularities, true, 1*time.Second)
	e.Start()
}
