package membership

import "github.com/nm-morais/go-babel/pkg/peer"

type DemmonTreeConfig = struct {
	gParentLatencyIncreaseThreshold uint64
	nrSamplesForLatency             uint64
	landmarks                       []peer.Peer
}
