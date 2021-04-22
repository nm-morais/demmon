package main

import (
	"crypto/rand"
	"encoding/csv"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"

	membershipProtocol "github.com/nm-morais/demmon/core/membership/protocol"
	"github.com/nm-morais/go-babel/pkg/peer"
)

func GetLocalIP() net.IP {

	itf, _ := net.InterfaceByName("eth0") //here your interface
	item, _ := itf.Addrs()
	var ip net.IP
	for _, addr := range item {
		switch v := addr.(type) {
		case *net.IPNet:
			if !v.IP.IsLoopback() {
				if v.IP.To4() != nil { //Verify if IP is IPV4
					ip = v.IP
					return ip
				}
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

func GetUseBWEnvVar() bool {
	useBw, ok := os.LookupEnv(UseBandwidthEnvName)
	if !ok {
		return false
	}
	return useBw == "true"
}

func GetBWScore() int {
	bwStr, ok := os.LookupEnv(BandwidthScoreEnvName)
	if !ok {
		return 0
	}
	bw, err := strconv.ParseInt(bwStr, 10, 32)
	if err != nil {
		panic(err)
	}
	return int(bw)
}

func GetWaitForStartEnvVar() bool {
	waitForStartEnvVarVal, exists := os.LookupEnv(WaitForStartEnvVar)
	if !exists {
		return false
	}
	return waitForStartEnvVarVal == "true"
}

func GetBenchmarkMembershipEnvVar() bool {
	benchmarkEnvVarVal, exists := os.LookupEnv(BenchmarkMembershipEnvName)
	if !exists {
		return false
	}
	return benchmarkEnvVarVal == "true"
}

func GetDemmonBenchmarkTypeEnvVar() (string, bool) {
	return os.LookupEnv(BenchmarkDemmonTypeEnvName)
}

func GetBenchmarkDemmonEnvVar() bool {
	benchmarkEnvVarVal, exists := os.LookupEnv(BenchmarkDemmonEnvName)
	if !exists {
		return false
	}
	return benchmarkEnvVarVal == "true"
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
			0,
			0,
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

func setupCSVWriter(folder, fileName string, headers []string) *csv.Writer {
	err := os.MkdirAll(folder, 0777)
	if err != nil {
		panic(err)
	}
	allLogsFile, err := os.Create(folder + fileName)
	if err != nil {
		panic(err)
	}

	writer := csv.NewWriter(allLogsFile)
	writeOrPanic(writer, headers)
	return writer
}
