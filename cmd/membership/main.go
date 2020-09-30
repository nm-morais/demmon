package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	_ "net/http/pprof"
	"time"

	"github.com/nm-morais/DeMMon/internal/membership"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/stream"
)

func main() {

	rand.Seed(time.Now().Unix() + rand.Int63())

	minProtosPort := 7000
	maxProtosPort := 8000

	minAnalyticsPort := 8000
	maxAnalyticsPort := 9000

	var protosPortVar int
	var analyticsPortVar int
	var randProtosPort bool
	var randAnalyticsPort bool
	var cpuprofile bool
	var memprofile bool

	flag.IntVar(&protosPortVar, "protos", 1200, "protos")
	flag.BoolVar(&randProtosPort, "rprotos", false, "port")
	flag.IntVar(&analyticsPortVar, "analytics", 1201, "analytics")
	flag.BoolVar(&randAnalyticsPort, "ranalytics", false, "port")

	flag.BoolVar(&cpuprofile, "cpuprofile", false, "cpuprofile")
	flag.BoolVar(&memprofile, "memprofile", false, "memprofile")

	flag.Parse()

	if randProtosPort {
		protosPortVar = rand.Intn(maxProtosPort-minProtosPort) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = rand.Intn(maxAnalyticsPort-minAnalyticsPort) + minAnalyticsPort
	}

	// PROTO MANAGER CONFS

	protoManagerConf := pkg.ProtocolManagerConfig{
		Cpuprofile:       cpuprofile,
		Memprofile:       memprofile,
		LogFolder:        "/code/logs/",
		HandshakeTimeout: 3 * time.Second,
		DialTimeout:      3 * time.Second,
		Peer:             peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	// protoManagerConf := pkg.ProtocolManagerConfig{
	// 	Cpuprofile:       cpuprofile,
	// 	Memprofile:       cpuprofile,
	// 	LogFolder:        "/tmp/demmon_logs/",
	// 	HandshakeTimeout: 3 * time.Second,
	// 	DialTimeout:      3 * time.Second,
	// 	Peer:             peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	// }

	// NODE WATCHER CONFS

	nodeWatcherConf := pkg.NodeWatcherConf{
		PrintLatencyToInterval:    2 * time.Second,
		MaxRedials:                2,
		HbTickDuration:            750 * time.Millisecond,
		NrMessagesWithoutWait:     3,
		NewLatencyWeight:          0.25,
		NrTestMessagesToSend:      1,
		NrTestMessagesToReceive:   1,
		OldLatencyWeight:          0.75,
		TcpTestTimeout:            3 * time.Second,
		UdpTestTimeout:            3 * time.Second,
		MinSamplesFaultDetector:   3,
		WindowSize:                10,
		EvalConditionTickDuration: 1500 * time.Millisecond,
		MinSamplesLatencyEstimate: 3,
	}

	// landmarks := []membership.PeerWithId{
	// 	membership.NewPeerWithId([membership.IdSegmentLen]byte{0, 0, 0, 0, 0, 0, 1}, peer.NewPeer(net.IPv4(10, 171, 238, 164), 1200, 1300), 0),
	// 	membership.NewPeerWithId([membership.IdSegmentLen]byte{0, 0, 0, 0, 0, 1, 0}, peer.NewPeer(net.IPv4(10, 171, 238, 164), 1201, 1301), 0),
	// 	membership.NewPeerWithId([membership.IdSegmentLen]byte{0, 0, 0, 0, 0, 1, 1}, peer.NewPeer(net.IPv4(10, 171, 238, 164), 1202, 1302), 0),
	// }

	landmarks := []*membership.PeerWithIdChain{
		membership.NewPeerWithIdChain(membership.PeerIDChain{membership.PeerID{12}}, peer.NewPeer(net.IPv4(10, 10, 1, 16), 1200, 1300), 0, 0),
		membership.NewPeerWithIdChain(membership.PeerIDChain{membership.PeerID{17}}, peer.NewPeer(net.IPv4(10, 10, 69, 22), 1200, 1300), 0, 0),
		membership.NewPeerWithIdChain(membership.PeerIDChain{membership.PeerID{23}}, peer.NewPeer(net.IPv4(10, 10, 5, 25), 1200, 1300), 0, 0),
	}

	// DEMMON TREE CONFS

	demmonTreeConf := membership.DemmonTreeConfig{
		LandmarkRedialTimer:               1 * time.Second,
		JoinMessageTimeout:                4 * time.Second,
		MaxTimeToProgressToNextLevel:      4 * time.Second,
		MaxRetriesJoinMsg:                 3,
		Landmarks:                         landmarks,
		MinGrpSize:                        2,
		MaxGrpSize:                        4,
		NrPeersToAbsorb:                   2,
		NrPeersToConsiderAsParentToAbsorb: 3,

		LimitFirstLevelGroupSize:      true,
		CheckChildenSizeTimerDuration: 10 * time.Second,
		ParentRefreshTickDuration:     3 * time.Second,
		ChildrenRefreshTickDuration:   3 * time.Second,
		RejoinTimerDuration:           10 * time.Second,

		MinLatencyImprovementToImprovePosition: 25 * time.Millisecond,
		AttemptImprovePositionProbability:      0.33,
		EvalMeasuredPeersRefreshTickDuration:   5 * time.Second,

		EmitWalkProbability:                0.33,
		BiasedWalkProbability:              0.2,
		BiasedWalkTTL:                      5,
		RandomWalkTTL:                      4,
		EmitWalkTimeout:                    5 * time.Second,
		MaxPeersInEView:                    15,
		MeasureNewPeersRefreshTickDuration: 5 * time.Second,
		MeasuredPeersSize:                  10,
		NrHopsToIgnoreWalk:                 2,
		NrPeersInWalkMessage:               10,
		NrPeersToMeasure:                   2,
		NrPeersToMergeInWalkSample:         4,

		CheckSwitchOportunityTimeout:          7500 * time.Millisecond,
		MinLatencyImprovementPerPeerForSwitch: 10 * time.Millisecond,
	}

	fmt.Println("Self peer: ", protoManagerConf.Peer.String())
	pkg.InitProtoManager(protoManagerConf)
	pkg.InitNodeWatcher(nodeWatcherConf)
	pkg.RegisterProtocol(membership.NewDemmonTree(demmonTreeConf))
	pkg.RegisterListener(stream.NewTCPListener(&net.TCPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())}))
	pkg.RegisterListener(stream.NewUDPListener(&net.UDPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())}))
	pkg.Start()
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
