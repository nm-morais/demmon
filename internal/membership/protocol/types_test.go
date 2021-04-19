package protocol_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	. "github.com/nm-morais/demmon/internal/membership/protocol"
	"github.com/nm-morais/go-babel/pkg/peer"
)

func TestSerializePeerWithIDChain(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	toSerialize := NewPeerWithIDChain(
		chain,
		peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300),
		200,
		10,
		Coordinates{0, 1, 2, 3}, 0, 0,
	)
	PwIDbytes := toSerialize.MarshalWithFields()
	_, deserialized := UnmarshalPeerWithIDChain(PwIDbytes)

	t.Logf("%+v,", toSerialize)
	t.Logf("%+v,", deserialized)
	fmt.Printf("%+v\n", toSerialize)
	fmt.Printf("%+v\n", deserialized)

	for i := 0; i < len(toSerialize.Chain()); i++ {
		currSegment := toSerialize.Chain()[i]
		currSegment2 := deserialized.Chain()[i]

		t.Logf("%+v,", currSegment)
		t.Logf("%+v,", currSegment2)

		for i := 0; i < len(currSegment); i++ {

			if currSegment[i] != currSegment2[i] {
				t.Logf("%+v,", toSerialize)
				t.Logf("%+v,", deserialized)
				t.FailNow()
			}
		}
	}

	for i := 0; i < len(toSerialize.Coordinates); i++ {
		c1 := toSerialize.Coordinates[i]
		c2 := deserialized.Coordinates[i]
		if c1 != c2 {

			t.FailNow()
		}
	}

	if !peer.PeersEqual(toSerialize, deserialized.Peer) {
		t.FailNow()
	}
	t.Logf("before: %+v", toSerialize.Coordinates)

	t.Logf("after: %+v", deserialized.Coordinates)
	t.FailNow()
}

func TestIsDescendantOf(t *testing.T) {
	ascendantChain := PeerIDChain{}
	ascendantChain = append(ascendantChain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	descendantChain := append(ascendantChain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	descendent := NewPeerWithIDChain(
		descendantChain,
		peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300),
		200,
		10,
		Coordinates{}, 0, 0,
	)

	if !descendent.IsDescendentOf(ascendantChain) {
		t.Errorf("%+v is not descendent of %+v", descendantChain, ascendantChain)
		t.FailNow()
	}

	if ascendantChain.IsDescendentOf(descendantChain) {
		t.FailNow()
	}
}

func TestIsDescendantOf2(t *testing.T) {
	ascendantChain := append(PeerIDChain{}, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 1, 1})
	descendantChain := append(PeerIDChain{}, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 0})
	descendent := NewPeerWithIDChain(
		descendantChain,
		peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300),
		200,
		10,
		Coordinates{}, 0, 0,
	)

	t.Log(ascendantChain)
	t.Log(descendantChain)

	if descendent.IsDescendentOf(ascendantChain) {
		t.Logf("here1")
		t.FailNow()
	}

	if ascendantChain.IsDescendentOf(descendantChain) {
		t.Logf("here2")
		t.FailNow()
	}
}

func TestIsEqual(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})
	peer1 := NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 1}, 0, 0)
	peer2 := NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 1}, 0, 0)
	if !peer.PeersEqual(peer1, peer2) {
		t.FailNow()
	}

	peer1 = nil
	peer2 = nil

	if peer.PeersEqual(peer1, peer2) {
		t.FailNow()
	}
}

func TestIsBetterThan(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})
	peer1 := NewMeasuredPeer(NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 1}, 0, 0), time.Duration(10*time.Millisecond))
	peer2 := NewMeasuredPeer(NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 1}, 0, 0), time.Duration(70*time.Millisecond))
	if !peer1.HasBetterLatencyThan(peer2, 10*time.Millisecond) {
		t.FailNow()
	}
	if peer2.HasBetterLatencyThan(peer1, 10*time.Millisecond) {
		t.FailNow()
	}
	if peer1.HasBetterLatencyThan(peer1, 100*time.Millisecond) {
		t.FailNow()
	}
	if peer1.HasBetterLatencyThan(peer1, 30*time.Millisecond) {
		t.FailNow()
	}
}

func TestEuclideanDist(t *testing.T) {

	c1 := Coordinates{5, 5}
	c2 := Coordinates{10, 10}

	dist, err := EuclideanDist(c1, c2)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	t.Logf("%f", dist)
}
