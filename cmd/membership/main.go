package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/nm-morais/DeMMon/internal/membership"
	"github.com/nm-morais/go-babel/configs"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/stream"
)

func main() {
	rand.Seed(time.Now().Unix() + rand.Int63())
	minPort := 8000
	maxPort := 9000

	var portVar int
	var randPort bool
	flag.IntVar(&portVar, "p", -1, "port")
	flag.BoolVar(&randPort, "r", false, "port")
	flag.Parse()

	if randPort {
		portVar = rand.Intn(maxPort-minPort) + minPort
	}

	/*
		listenAddrTcp, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", GetLocalIP(), portVar))
		if err != nil {
			panic(err)
		}
	*/

	listenAddrTcp, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", portVar))
	if err != nil {
		panic(err)
	}

	listenAddrUdp, err := net.ResolveUDPAddr("udp", fmt.Sprintf("localhost:%d", portVar))
	if err != nil {
		panic(err)
	}

	config := configs.ProtocolManagerConfig{ // TODO extract from JSON would be okay
		LogFolder:             "logs",
		HandshakeTimeout:      1 * time.Second,
		HeartbeatTickDuration: 1 * time.Second,
		DialTimeout:           1 * time.Second,
		ConnectionReadTimeout: 5 * time.Second,
	}

	pkg.InitProtoManager(config, listenAddrTcp)

	landmarksStr := []string{"localhost:1200", "localhost:1201", "localhost:1202"}
	//landmarksStr := []string{"10.10.0.17:1200", "10.10.68.23:1200", "10.10.0.61:1200", "10.10.0.154:1200", "10.10.0.29:1200", "10.10.0.189:1200"}
	landmarks := make([]peer.Peer, 0, len(landmarksStr))

	for _, landmarkStr := range landmarksStr {
		landmarkAddr, err := net.ResolveTCPAddr("tcp", landmarkStr)
		if err != nil {
			panic(err)
		}
		peer := peer.NewPeer(landmarkAddr)
		landmarks = append(landmarks, peer)
	}

	demmonTreeConf := membership.DemmonTreeConfig{
		ParentRefreshTickDuration:       1 * time.Second,
		MaxRetriesJoinMsg:               3,
		BootstrapRetryTimeout:           1 * time.Second,
		GParentLatencyIncreaseThreshold: 200,
		Landmarks:                       landmarks,
		NrSamplesForLatency:             5,
		MaxRetriesForLatency:            5,
	}

	pkg.RegisterListener(stream.NewTCPListener(listenAddrTcp))
	pkg.RegisterListener(stream.NewUDPListener(listenAddrUdp))

	pkg.RegisterProtocol(membership.NewDemmonTree(demmonTreeConf))

	pkg.Start()
}

func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
