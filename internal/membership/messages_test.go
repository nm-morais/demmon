package membership

import (
	"fmt"
	"net"
	"testing"

	"github.com/nm-morais/go-babel/pkg/peer"
)

func TestJoinAsParentMsgSerializer(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})
	toSerialize := NewJoinAsParentMessage(PeerIDChain{PeerID{0, 0, 0, 1, 1, 0, 0, 1}}, chain, 10, nil)
	serializer := joinAsParentMsgSerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	msgConverted := deserialized.(joinAsParentMessage)
	fmt.Println(msgConverted)
	if msgConverted.Level != toSerialize.Level {
		t.Log("levels do not match")
		t.FailNow()
		return
	}

	if !ChainsEqual(chain, msgConverted.ProposedId) {
		t.Log("chains not match")
		t.FailNow()
		return
	}
}

func TestUpdateParentMsgSerializer(t *testing.T) {

	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})
	toSerialize := NewUpdateParentMessage(peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 10, chain, nil)
	serializer := updateParentMsgSerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	msgConverted := deserialized.(updateParentMessage)
	fmt.Println(msgConverted)
	if msgConverted.ParentLevel != toSerialize.ParentLevel {
		t.Log("levels do not match")
		t.FailNow()
		return
	}

	if !ChainsEqual(chain, msgConverted.ProposedIdChain) {
		t.Log("chains not match")
		t.FailNow()
		return
	}

	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", deserialized)
}

func TestAbsorbMessageSerializer(t *testing.T) {

	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	peer1 := NewMeasuredPeer(NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3), 10)
	peer2 := NewMeasuredPeer(NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3), 10)
	peer3 := NewMeasuredPeer(NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3), 10)

	peersToAbsorb := MeasuredPeersByLat{peer1, peer2, peer3}

	toSerialize := NewAbsorbMessage(peersToAbsorb, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300))
	serializer := absorbMessageSerializer
	msgBytes := serializer.Serialize(toSerialize)
	t.Logf("%+v", msgBytes)
	deserialized := serializer.Deserialize(msgBytes)
	msgConverted := deserialized.(absorbMessage)
	fmt.Println(msgConverted)
	if !msgConverted.PeerAbsorber.Equals(toSerialize.PeerAbsorber) {
		t.Logf("%+v", msgConverted.PeerAbsorber.ToString())
		t.Logf("%+v", toSerialize.PeerAbsorber.ToString())
		t.Log("peerAbsorber does not match")
		t.FailNow()
		return
	}

	if !peer1.Equals(msgConverted.PeersToAbsorb[0]) {
		t.Logf("%+v", peer1.ToString())
		t.Logf("%+v", msgConverted.PeersToAbsorb[0].ToString())
		t.Log("peer1 does not match")
		t.FailNow()
		return
	}

	if !peer2.Equals(msgConverted.PeersToAbsorb[1]) {
		t.Logf("%+v", peer2)
		t.Logf("%+v", msgConverted.PeersToAbsorb[1])
		t.Log("peer2 does not match")
		t.FailNow()
		return
	}

	if !peer3.Equals(msgConverted.PeersToAbsorb[2]) {
		t.Logf("%s", peer3.ToString())
		t.Logf("%+v", msgConverted.PeersToAbsorb[2])
		t.Log("peer3 does not match")
		t.FailNow()
		return
	}

	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", deserialized)
}

func TestJoinReplyMsgSerializer(t *testing.T) {
	children := []PeerWithId{
		NewPeerWithId(PeerID{1, 1, 1, 1, 1, 0}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 0),
		NewPeerWithId(PeerID{1, 1, 1, 1, 1, 1, 1, 1}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 0),
		NewPeerWithId(PeerID{0, 0, 0, 0, 0}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 0),
	}

	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	toSerialize := NewJoinReplyMessage(children, 10, 100, chain)
	serializer := joinReplyMsgSerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	msgConverted := deserialized.(joinReplyMessage)
	fmt.Println(msgConverted)
	if msgConverted.Level != toSerialize.Level {
		t.Log("levels do not match")
		t.FailNow()
		return
	}

	if msgConverted.Level != toSerialize.Level {
		t.Log("measured latency does not match")
		t.FailNow()
		return
	}

	if !ChainsEqual(chain, msgConverted.IdChain) {
		t.Log("chains not match")
		t.FailNow()
		return
	}

	for i := 0; i < len(children); i++ {
		curr := children[i]
		curr2 := msgConverted.Children[i]
		if curr.ID() != curr2.ID() {
			t.Log("ids not match")
			t.FailNow()
			return
		}
		if !curr.Equals(curr2) {
			t.Log("peers not match")
			t.FailNow()
			return
		}
	}
}
