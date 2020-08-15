package membership

import "github.com/nm-morais/go-babel/pkg/peer"

type DemmonTreeConfig = struct {
	GParentLatencyIncreaseThreshold uint64
	NrSamplesForLatency             uint64
	Landmarks                       []peer.Peer
}
