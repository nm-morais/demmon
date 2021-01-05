package utils

import (
	"crypto/rand"
	"math"
	"math/big"

	"github.com/nm-morais/go-babel/pkg/peer"
)

func GetRandInt(max int) int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		panic(err)
	}

	return n.Int64()
}

func GetRandFloat64() float64 {
	n, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(err)
	}

	return float64(n.Int64()) / float64(math.MaxInt64)
}

func PeerArrContains(pArr []peer.Peer, toFind peer.Peer) bool {
	for _, p := range pArr {
		if peer.PeersEqual(p, toFind) {
			return true
		}
	}

	return false
}
