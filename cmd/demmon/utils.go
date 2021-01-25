package main

import (
	"crypto/rand"
	"math/big"
	"net"
	"os"
	"strings"

	membershipProtocol "github.com/nm-morais/demmon/internal/membership/protocol"
	"github.com/nm-morais/go-babel/pkg/peer"
)

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

func GetWaitForStartEnvVar() bool {
	waitForStartEnvVarVal, exists := os.LookupEnv(WaitForStartEnvVar)
	if !exists {
		return false
	}
	return waitForStartEnvVarVal == "true"
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
			make(membershipProtocol.Coordinates, len(landmarksSplitted)),
		)
		landmarks[i] = landmark
	}
	return landmarks, true
}

func getRandInt(max int64) int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		panic(err)
	}

	return n.Int64()
}
