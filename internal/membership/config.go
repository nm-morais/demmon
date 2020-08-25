package membership

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
)

type DemmonTreeConfig = struct {
	MaxRetriesJoinMsg               int
	ParentRefreshTickDuration       time.Duration
	GParentLatencyIncreaseThreshold time.Duration
	NrSamplesForLatency             int
	MaxRetriesForLatency            int
	BootstrapRetryTimeout           time.Duration
	Landmarks                       []peer.Peer
}
