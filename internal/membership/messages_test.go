package membership

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
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

	if !chain.Equal(msgConverted.ProposedId) {
		t.Log("chains not match")
		t.FailNow()
		return
	}
}

func TestJoinAsChildMsgReplySerializer(t *testing.T) {
	chain := PeerIDChain{}
	proposedId := PeerID{0, 0, 0, 1, 1, 0, 0, 1}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	peer1 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)
	peer2 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)
	peer3 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)
	peer4 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)
	toSerialize := NewJoinAsChildMessageReply(true, proposedId, 10, peer1, []*PeerWithIdChain{peer2, peer3}, peer4)
	serializer := joinAsChildMessageReplySerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	converted := deserialized.(joinAsChildMessageReply)

	if !reflect.DeepEqual(converted, toSerialize) {
		t.Logf("%+v", converted)
		t.Logf("%+v", toSerialize)
		t.FailNow()
	}
}

func TestUpdateParentMsgSerializer(t *testing.T) {

	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	childrenId := PeerID{0, 0, 0, 1, 1, 0, 0, 1}

	grandparent := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)
	parent := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)

	peer1 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)
	peer2 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)
	peer3 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)

	siblings := []*PeerWithIdChain{peer1, peer2, peer3}

	toSerialize := NewUpdateParentMessage(grandparent, parent, 10, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, siblings)
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

	if !bytes.Equal(msgConverted.ProposedId[:], childrenId[:]) {
		t.Log("chains not match")
		t.FailNow()
		return
	}

	i := 0
	for _, sibling := range msgConverted.Siblings {
		if !peer.PeersEqual(siblings[i], sibling) {
			t.Log(sibling)
			t.FailNow()
			return
		}
		i++
	}

	if !peer.PeersEqual(msgConverted.GrandParent, grandparent) {
		t.Log("grandparents not equal")
		t.FailNow()
		return
	}
	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", msgConverted)
	t.FailNow()

}

func TestAbsorbMessageSerializer(t *testing.T) {

	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	peer1 := NewMeasuredPeer(NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0), 10)
	peer2 := NewMeasuredPeer(NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 1000), 10)
	peer3 := NewMeasuredPeer(NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 3), 10)

	absorber := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 3)

	peersToAbsorb := MeasuredPeersByLat{peer1, peer2, peer3}

	toSerialize := NewAbsorbMessage(peersToAbsorb, absorber)
	serializer := absorbMessageSerializer
	msgBytes := serializer.Serialize(toSerialize)
	t.Logf("%+v", msgBytes)
	deserialized := serializer.Deserialize(msgBytes)
	msgConverted := deserialized.(absorbMessage)
	fmt.Println(msgConverted)
	if !peer.PeersEqual(msgConverted.PeerAbsorber, toSerialize.PeerAbsorber) {
		t.Logf("%+v", msgConverted.PeerAbsorber.String())
		t.Logf("%+v", toSerialize.PeerAbsorber.String())
		t.Log("peerAbsorber does not match")
		t.FailNow()
		return
	}

	if !peer.PeersEqual(msgConverted.PeersToAbsorb[0], peer1) {
		t.Logf("%+v", peer1.String())
		t.Logf("%+v", msgConverted.PeersToAbsorb[0].String())
		t.Log("peer1 does not match")
		t.FailNow()
		return
	}

	if !peer.PeersEqual(peer2, msgConverted.PeersToAbsorb[1]) {
		t.Logf("%+v", peer2)
		t.Logf("%+v", msgConverted.PeersToAbsorb[1])
		t.Log("peer2 does not match")
		t.FailNow()
		return
	}

	if !peer.PeersEqual(peer3, msgConverted.PeersToAbsorb[2]) {
		t.Logf("%s", peer3.String())
		t.Logf("%+v", msgConverted.PeersToAbsorb[2])
		t.Log("peer3 does not match")
		t.FailNow()
		return
	}

	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", deserialized)
}

func TestJoinReplyMsgSerializer(t *testing.T) {

	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	self := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0)

	children := []*PeerWithIdChain{
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0),
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0),
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0),
	}

	toSerialize := NewJoinReplyMessage(children, 10, self)
	serializer := joinReplyMsgSerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	msgConverted := deserialized.(joinReplyMessage)
	fmt.Printf("%+v\n", msgConverted)
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

	if !peer.PeersEqual(msgConverted.Sender, toSerialize.Sender) {
		t.Log("Self does match")
		t.Log(msgConverted.Sender)
		t.Log(toSerialize.Sender)
		t.FailNow()
		return
	}

	for i := 0; i < len(children); i++ {
		curr := children[i]
		curr2 := msgConverted.Children[i]
		if !curr.Chain().Equal(curr2.Chain()) {
			t.Log(curr.Chain())
			t.Log(curr2.Chain())
			t.Log("chains not match")
			t.FailNow()
			return
		}
		if !peer.PeersEqual(curr, curr2.Peer) {
			t.Log("peers not match")
			t.FailNow()
			return
		}
	}
}
