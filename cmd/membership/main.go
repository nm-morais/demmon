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

	protoManagerConf := pkg.ProtocolManagerConfig{
		LogFolder:             "/Users/nunomorais/go/src/github.com/nm-morais/go-babel/logs/",
		HandshakeTimeout:      1 * time.Second,
		HeartbeatTickDuration: 1 * time.Second,
		DialTimeout:           1 * time.Second,
		ConnectionReadTimeout: 5 * time.Second,
		Peer:                  peer.NewPeer(GetLocalIP(), uint16(protosPortVar), uint16(analyticsPortVar)),
	}

	fmt.Println("Self peer: ", protoManagerConf.Peer.ToString())

	nodeWatcherConf := pkg.NodeWatcherConf{
		MaxRedials:              3,
		HbTickDuration:          1 * time.Second,
		MinSamplesFaultDetector: 5,
		NewLatencyWeight:        0.1,
		NrTestMessagesToSend:    3,
		NrTestMessagesToReceive: 1,
		OldLatencyWeight:        0.9,
		TcpTestTimeout:          5 * time.Second,
		UdpTestTimeout:          5 * time.Second,
		WindowSize:              5,
	}

	landmarks := []peer.Peer{
		peer.NewPeer(net.IPv4(127, 0, 0, 1), 1200, 1300),
		peer.NewPeer(net.IPv4(127, 0, 0, 1), 1201, 1301),
		peer.NewPeer(net.IPv4(127, 0, 0, 1), 1202, 1302),
	}

	demmonTreeConf := membership.DemmonTreeConfig{
		MaxTimeToProgressToNextLevel:    5 * time.Second,
		ParentRefreshTickDuration:       1 * time.Second,
		MaxRetriesJoinMsg:               3,
		BootstrapRetryTimeout:           1 * time.Second,
		GParentLatencyIncreaseThreshold: 15 * time.Millisecond,
		Landmarks:                       landmarks,
		NrSamplesForLatency:             5,
		MaxRetriesForLatency:            5,
	}

	pkg.InitProtoManager(protoManagerConf)
	pkg.RegisterListener(stream.NewTCPListener(&net.TCPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())}))
	pkg.RegisterListener(stream.NewUDPListener(&net.UDPAddr{IP: protoManagerConf.Peer.IP(), Port: int(protoManagerConf.Peer.ProtosPort())}))
	pkg.InitNodeWatcher(nodeWatcherConf)
	pkg.RegisterProtocol(membership.NewDemmonTree(demmonTreeConf))
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
