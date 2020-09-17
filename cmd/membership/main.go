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
		HandshakeTimeout: 1 * time.Second,
		DialTimeout:      1 * time.Second,
		Peer:             peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	// NODE WATCHER CONFS

	nodeWatcherConf := pkg.NodeWatcherConf{
		MaxRedials:                3,
		HbTickDuration:            300 * time.Millisecond,
		MinSamplesFaultDetector:   5,
		NrMessagesWithoutWait:     5,
		NewLatencyWeight:          0.1,
		NrTestMessagesToSend:      3,
		NrTestMessagesToReceive:   1,
		OldLatencyWeight:          0.9,
		TcpTestTimeout:            5 * time.Second,
		UdpTestTimeout:            5 * time.Second,
		WindowSize:                5,
		EvalConditionTickDuration: 500 * time.Millisecond,
		MinSamplesLatencyEstimate: 5,
	}

	landmarks := []membership.PeerWithId{
		membership.NewPeerWithId([membership.IdSegmentLen]byte{0, 0, 0, 0, 0, 0, 1}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 0),
		membership.NewPeerWithId([membership.IdSegmentLen]byte{0, 0, 0, 0, 0, 1, 0}, peer.NewPeer(net.IPv4(10, 10, 68, 23), 1200, 1300), 0),
		membership.NewPeerWithId([membership.IdSegmentLen]byte{0, 0, 0, 0, 0, 1, 1}, peer.NewPeer(net.IPv4(10, 10, 4, 26), 1200, 1300), 0),
	}

	// DEMMON TREE CONFS

	demmonTreeConf := membership.DemmonTreeConfig{
		MaxTimeToProgressToNextLevel:        5 * time.Second,
		MaxRetriesJoinMsg:                   3,
		MinLatencyImprovementToBecomeParent: 20 * time.Millisecond,
		Landmarks:                           landmarks,
		MinGrpSize:                          3,
		MaxGrpSize:                          5,
		LimitFirstLevelGroupSize:            false,
		CheckChildenSizeTimerDuration:       5 * time.Second,
		ParentRefreshTickDuration:           3 * time.Second,
		ChildrenRefreshTickDuration:         3 * time.Second,
		RejoinTimerDuration:                 10 * time.Second,

		AttemptImprovePositionProbability:      0.2,
		EvalMeasuredPeersRefreshTickDuration:   5 * time.Second,
		EmitWalkProbability:                    1,
		BiasedWalkProbability:                  0.2,
		BiasedWalkTTL:                          7,
		EmitWalkTimeout:                        5 * time.Second,
		MaxPeersInEView:                        10,
		MeasureNewPeersRefreshTickDuration:     5 * time.Second,
		MeasuredPeersSize:                      5,
		MinLatencyImprovementToImprovePosition: 20 * time.Millisecond,
		NrHopsToIgnoreWalk:                     2,
		NrPeersInWalkMessage:                   10,
		NrPeersToMeasure:                       3,
		NrPeersToMergeInWalkSample:             2,
		RandomWalkTTL:                          6,
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
