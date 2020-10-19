package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"time"

	exporter "github.com/nm-morais/demmon-exporter"
	"github.com/nm-morais/demmon/internal/frontend"
	"github.com/nm-morais/demmon/internal/membership"
	"github.com/nm-morais/demmon/internal/monitoring/importer"
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
	demmonTreeConf = membership.DemmonTreeConfig{
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

	landmarks = []*membership.PeerWithIdChain{
		membership.NewPeerWithIdChain(membership.PeerIDChain{membership.PeerID{12}}, peer.NewPeer(net.IPv4(10, 10, 1, 16), 1200, 1300), 0, 0, make(membership.Coordinates, 4)),
		membership.NewPeerWithIdChain(membership.PeerIDChain{membership.PeerID{17}}, peer.NewPeer(net.IPv4(10, 10, 69, 22), 1200, 1300), 0, 0, make(membership.Coordinates, 4)),
		membership.NewPeerWithIdChain(membership.PeerIDChain{membership.PeerID{23}}, peer.NewPeer(net.IPv4(10, 10, 5, 25), 1200, 1300), 0, 0, make(membership.Coordinates, 4)),
		membership.NewPeerWithIdChain(membership.PeerIDChain{membership.PeerID{23}}, peer.NewPeer(net.IPv4(10, 10, 73, 153), 1200, 1300), 0, 0, make(membership.Coordinates, 4)),
	}

	smConf = pkg.StreamManagerConf{
		DialTimeout: 3 * time.Second,
	}

	protosPortVar     int
	analyticsPortVar  int
	randProtosPort    bool
	randAnalyticsPort bool
	cpuprofile        bool
	memprofile        bool
	cli               bool
)

func main() {

	rand.Seed(time.Now().Unix() + rand.Int63())

	flag.IntVar(&protosPortVar, "protos", 1200, "protos")
	flag.BoolVar(&cli, "cli", false, "cli")

	flag.BoolVar(&randProtosPort, "rprotos", false, "port")
	flag.IntVar(&analyticsPortVar, "analytics", 1201, "analytics")
	flag.BoolVar(&randAnalyticsPort, "ranalytics", false, "port")

	flag.BoolVar(&cpuprofile, "cpuprofile", false, "cpuprofile")
	flag.BoolVar(&memprofile, "memprofile", false, "memprofile")

	flag.Parse()

	protoManagerConf := pkg.ProtocolManagerConfig{
		Cpuprofile:       cpuprofile,
		Memprofile:       memprofile,
		LogFolder:        "/code/logs/",
		HandshakeTimeout: 3 * time.Second,
		Peer:             peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	exporterConfs := exporter.ExporterConf{
		ExportFrequency: 5 * time.Second,
		ImporterAddr:    protoManagerConf.Peer,
		MaxRedials:      3,
		RedialTimeout:   3 * time.Second,
	}

	if randProtosPort {
		protosPortVar = rand.Intn(maxProtosPort-minProtosPort) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = rand.Intn(maxAnalyticsPort-minAnalyticsPort) + minAnalyticsPort
	}

	// DEMMON TREE CONFS
	fmt.Println("Self peer: ", protoManagerConf.Peer.String())
	p := pkg.NewProtoManager(protoManagerConf, smConf)
	nw := pkg.NewNodeWatcher(nodeWatcherConf, p)
	e := exporter.New(exporterConfs, p)
	setupDemmonMetrics(e)
	f := frontend.New(p)
	p.RegisterListenAddr(protoManagerConf.Peer.ToTCPAddr())
	p.RegisterListenAddr(protoManagerConf.Peer.ToUDPAddr())
	p.RegisterNodeWatcher(nw)
	p.RegisterProtocol(e.Proto())
	p.RegisterProtocol(f.Proto())
	p.RegisterProtocol(importer.New(p))
	p.RegisterProtocol(membership.New(demmonTreeConf, p, nw))

	if cli {
		go p.Start()
		Repl(f)
	} else {
		fmt.Println("Starting in silent mode")
		p.Start()
	}
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
	e.NewGauge("goroutine_count", func() float64 { return float64(runtime.NumGoroutine()) })
}
