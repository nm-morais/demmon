package membership

import (
	"fmt"
	"net"
	"testing"

	"github.com/nm-morais/go-babel/pkg/peer"
)

func TestSerializePeerWithIDChain(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	toSerialize := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 200, 10)
	PwIDbytes := toSerialize.MarshalWithFields()
	_, deserialized := UnmarshalPeerWithIdChain(PwIDbytes)
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

	if !peer.PeersEqual(toSerialize, deserialized.Peer) {
		t.FailNow()
	}
	t.Logf("before: %+v", toSerialize.version)
	t.Logf("after: %+v", deserialized.version)

}

func TestIsDescendantOf(t *testing.T) {
	ascendantChain := PeerIDChain{}
	ascendantChain = append(ascendantChain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	ascendantChain = append(ascendantChain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	descendantChain := append(ascendantChain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	descendent := NewPeerWithIdChain(descendantChain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 200, 10)

	if !descendent.IsDescendentOf(ascendantChain) {
		t.FailNow()
	}

	if descendent.IsDescendentOf(descendantChain) {
		t.FailNow()
	}

}

func TestIsEqual(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})
	peer1 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)
	peer2 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)
	if !peer.PeersEqual(peer1, peer2) {
		t.FailNow()
	}

	peer1 = nil
	peer2 = nil
	if !peer.PeersEqual(peer1, peer2) {
		t.FailNow()
	}
}
