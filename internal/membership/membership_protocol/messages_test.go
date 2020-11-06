package membership_protocol

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

	peer1 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 2, 0})
	// peer2 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{1, 3, 1})
	// peer3 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{2, 4, 3})
	// peer4 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 5, 5})
	toSerialize := NewJoinAsChildMessageReply(false, proposedId, 10, peer1, nil, nil)
	serializer := joinAsChildMessageReplySerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	converted := deserialized.(joinAsChildMessageReply)

	if !reflect.DeepEqual(converted, toSerialize) {
		t.Logf("%+v", converted)
		t.Logf("%+v", toSerialize)
		t.Logf("%+v", converted.GrandParent.Coordinates)
		t.Logf("%+v", toSerialize.GrandParent.Coordinates)
		t.FailNow()
	}
}

func TestUpdateParentMsgSerializer(t *testing.T) {

	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	childrenId := PeerID{0, 0, 0, 1, 1, 0, 0, 1}

	grandparent := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 2, 2})
	parent := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 1, 1})

	peer1 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 0, 0})
	peer2 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 0, 0})
	peer3 := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 0, 0})

	siblings := []*PeerWithIdChain{peer1, peer2, peer3}

	toSerialize := NewUpdateParentMessage(grandparent, parent, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, siblings)
	serializer := updateParentMsgSerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	msgConverted := deserialized.(updateParentMessage)
	fmt.Println(msgConverted)

	if !bytes.Equal(msgConverted.ProposedId[:], childrenId[:]) {
		t.Log("chains not match")
		t.FailNow()
		return
	}

	i := 0
	for _, sibling := range msgConverted.Siblings {
		if !peer.PeersEqual(siblings[i], sibling) {
			t.Log(sibling)
			return
		}
		i++
	}

	if !peer.PeersEqual(msgConverted.GrandParent, grandparent) {
		t.Log("grandparents not equal")
		return
	}
	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", msgConverted)

	t.Logf("before: %+v", toSerialize.GrandParent.StringWithFields())
	t.Logf("after: %+v", msgConverted.GrandParent.StringWithFields())

	t.Logf("before: %+v", toSerialize.Parent.StringWithFields())
	t.Logf("after: %+v", msgConverted.Parent.StringWithFields())
	t.FailNow()

}

func TestAbsorbMessageSerializer(t *testing.T) {

	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	absorber := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 3, Coordinates{3})
	absorbed := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 3, Coordinates{3})

	toSerialize := NewAbsorbMessage(absorbed, absorber)
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

	if !peer.PeersEqual(absorbed, msgConverted.PeerToKick) {
		t.Logf("%s", absorbed.String())
		t.Logf("%+v", msgConverted.PeerToKick)
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

	self := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3})

	children := []*PeerWithIdChain{
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3}),
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3}),
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3}),
	}

	toSerialize := NewJoinReplyMessage(children, self)
	serializer := joinReplyMsgSerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	msgConverted := deserialized.(joinReplyMessage)
	fmt.Printf("%+v\n", msgConverted)

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

func TestRandomWalkSerializer(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	sender := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 3, 3, 4})

	sample := []*PeerWithIdChain{
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 1, 1, 2}),
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 3, 4, 1}),
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 0, 0, 0}),
	}
	toSerialize := NewRandomWalkMessage(3, sender, sample)

	serializer := randomWalkMessageSerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	msgConverted := deserialized.(randomWalkMessage)
	fmt.Println(msgConverted)
	if !reflect.DeepEqual(sender, msgConverted.Sender) {
		t.Log("sender not equals")
		t.Log(sender.StringWithFields())
		t.Log(msgConverted.Sender.StringWithFields())
	}

	i := 0
	for _, p := range sample {
		t.Log(p.StringWithFields())
		t.Log(msgConverted.Sample[i].StringWithFields())
		i++
	}
	t.FailNow()

}

func TestRandomWalkReplySerializer(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	sample := []*PeerWithIdChain{
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 1, 1, 2}),
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 3, 4, 1}),
		NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 0, 0, 0}),
	}
	toSerialize := NewWalkReplyMessage(sample)

	serializer := walkReplyMessageSerializer
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := serializer.Deserialize(msgBytes)

	msgConverted := deserialized.(walkReplyMessage)
	fmt.Println(msgConverted)

	i := 0
	for _, p := range sample {
		t.Log(p.StringWithFields())
		t.Log(msgConverted.Sample[i].StringWithFields())
		i++
	}
	t.FailNow()
}
