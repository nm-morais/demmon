package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
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

	flag.IntVar(&protosPortVar, "protos", 1200, "protos")
	flag.BoolVar(&randProtosPort, "rprotos", false, "port")
	flag.IntVar(&analyticsPortVar, "analytics", 1201, "analytics")
	flag.BoolVar(&randAnalyticsPort, "ranalytics", false, "port")
	flag.Parse()

	if randProtosPort {
		protosPortVar = rand.Intn(maxProtosPort-minProtosPort) + minProtosPort
	}

	if randAnalyticsPort {
		analyticsPortVar = rand.Intn(maxAnalyticsPort-minAnalyticsPort) + minAnalyticsPort
	}

	// PROTO MANAGER CONFS

	protoManagerConf := pkg.ProtocolManagerConfig{
		LogFolder:        "/code/logs/",
		HandshakeTimeout: 3 * time.Second,
		DialTimeout:      3 * time.Second,
		Peer:             peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	// NODE WATCHER CONFS

	nodeWatcherConf := pkg.NodeWatcherConf{
		MaxRedials:                2,
		HbTickDuration:            750 * time.Millisecond,
		MinSamplesFaultDetector:   3,
		NrMessagesWithoutWait:     3,
		NewLatencyWeight:          0.1,
		NrTestMessagesToSend:      1,
		NrTestMessagesToReceive:   1,
		OldLatencyWeight:          0.9,
		TcpTestTimeout:            3 * time.Second,
		UdpTestTimeout:            3 * time.Second,
		WindowSize:                10,
		EvalConditionTickDuration: 1500 * time.Millisecond,
		MinSamplesLatencyEstimate: 5,
	}

	landmarks := []membership.PeerWithId{
		membership.NewPeerWithId([membership.IdSegmentLen]byte{0, 0, 0, 0, 0, 0, 1}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 0),
		membership.NewPeerWithId([membership.IdSegmentLen]byte{0, 0, 0, 0, 0, 1, 0}, peer.NewPeer(net.IPv4(10, 10, 68, 23), 1200, 1300), 0),
		membership.NewPeerWithId([membership.IdSegmentLen]byte{0, 0, 0, 0, 0, 1, 1}, peer.NewPeer(net.IPv4(10, 10, 4, 26), 1200, 1300), 0),
	}

	// DEMMON TREE CONFS

	demmonTreeConf := membership.DemmonTreeConfig{

		JoinMessageTimeout:                3 * time.Second,
		MaxTimeToProgressToNextLevel:      5 * time.Second,
		MaxRetriesJoinMsg:                 3,
		Landmarks:                         landmarks,
		MinGrpSize:                        2,
		MaxGrpSize:                        4,
		NrPeersToAbsorb:                   2,
		NrPeersToConsiderAsParentToAbsorb: 3,
		LimitFirstLevelGroupSize:          true,
		CheckChildenSizeTimerDuration:     10 * time.Second,
		ParentRefreshTickDuration:         3 * time.Second,
		ChildrenRefreshTickDuration:       3 * time.Second,
		RejoinTimerDuration:               10 * time.Second,

		AttemptImprovePositionProbability:      0.3,
		EvalMeasuredPeersRefreshTickDuration:   5 * time.Second,
		EmitWalkProbability:                    0.33,
		BiasedWalkProbability:                  0.2,
		BiasedWalkTTL:                          8,
		RandomWalkTTL:                          8,
		EmitWalkTimeout:                        5 * time.Second,
		MaxPeersInEView:                        20,
		MeasureNewPeersRefreshTickDuration:     5 * time.Second,
		MeasuredPeersSize:                      10,
		MinLatencyImprovementToImprovePosition: 20 * time.Millisecond,
		NrHopsToIgnoreWalk:                     2,
		NrPeersInWalkMessage:                   20,
		NrPeersToMeasure:                       3,
		NrPeersToMergeInWalkSample:             4,
	}

	fmt.Println("Self peer: ", protoManagerConf.Peer.ToString())
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
