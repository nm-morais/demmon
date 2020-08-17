package membership

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
)

type DemmonTreeConfig = struct {
	MaxRetriesJoinMsg               int
	ParentRefreshTickDuration       time.Duration
	GParentLatencyIncreaseThreshold time.Duration
	NrSamplesForLatency             uint64
	Landmarks                       []peer.Peer
}
