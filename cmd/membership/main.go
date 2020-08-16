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

	listenAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", portVar))
	if err != nil {
		panic(err)
	}
	config := configs.ProtocolManagerConfig{
		LogFolder:             "/Users/nunomorais/go/src/github.com/nm-morais/DeMMon/logs/",
		HandshakeTimeout:      1 * time.Second,
		HeartbeatTickDuration: 1 * time.Second,
		DialTimeout:           1 * time.Second,
		ConnectionReadTimeout: 5 * time.Second,
	}
	pkg.InitProtoManager(config, stream.NewTCPListener(listenAddr))

	landmarksStr := []string{"127.0.0.1:1200", "127.0.0.1:1700"}
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
		GParentLatencyIncreaseThreshold: 200,
		Landmarks:                       landmarks,
		NrSamplesForLatency:             5,
	}

	pkg.RegisterProtocol(membership.NewDemmonTree(demmonTreeConf))
	pkg.Start()
}
