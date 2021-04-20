package protocol_test

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"testing"

	. "github.com/nm-morais/demmon/internal/membership/protocol"
	"github.com/nm-morais/go-babel/pkg/peer"
)

func TestJoinAsParentMsgSerializer(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})
	toSerialize := NewJoinAsParentMessage(PeerIDChain{PeerID{0, 0, 0, 1, 1, 0, 0, 1}}, chain, 10, nil)
	serializer := toSerialize.Serializer()
	deserializer := toSerialize.Deserializer()
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := deserializer.Deserialize(msgBytes)
	msgConverted := deserialized.(JoinAsParentMessage)
	fmt.Println(msgConverted)

	if msgConverted.Level != toSerialize.Level {
		t.Log("levels do not match")
		t.FailNow()

		return
	}

	if !chain.Equal(msgConverted.ProposedID) {
		t.Log("chains not match")
		t.FailNow()

		return
	}
}

func TestJoinAsChildMsgReplySerializer(t *testing.T) {
	chain := PeerIDChain{}
	proposedID := PeerID{0, 0, 0, 1, 1, 0, 0, 1}

	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	peer1 := NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 2, 0}, 0, 0)

	toSerialize := NewJoinAsChildMessageReply(false, proposedID, 10, peer1, nil, nil)
	serializer := toSerialize.Serializer()
	deserializer := toSerialize.Deserializer()
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := deserializer.Deserialize(msgBytes)
	converted := deserialized.(JoinAsChildMessageReply)

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
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	childrenID := PeerID{0, 0, 0, 1, 1, 0, 0, 1}

	grandparent := NewPeerWithIDChain(
		chain,
		peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300),
		3,
		0,
		Coordinates{3, 2, 2},
		0, 0,
	)
	parent := NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 1, 1}, 0, 0)

	peer1 := NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 0, 0}, 0, 0)
	peer2 := NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 0, 0}, 0, 0)
	peer3 := NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 0, 0}, 0, 0)

	siblings := []*PeerWithIDChain{peer1, peer2, peer3}

	toSerialize := NewUpdateParentMessage(grandparent, parent, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, siblings)
	serializer := toSerialize.Serializer()
	deserializer := toSerialize.Deserializer()
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := deserializer.Deserialize(msgBytes)

	msgConverted := deserialized.(UpdateParentMessage)
	fmt.Println(msgConverted)

	if !bytes.Equal(msgConverted.ProposedID[:], childrenID[:]) {
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
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	toKick := NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 3, Coordinates{3}, 0, 0)

	toSerialize := NewAbsorbMessage(toKick)
	serializer := toSerialize.Serializer()
	deserializer := toSerialize.Deserializer()
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := deserializer.Deserialize(msgBytes)
	msgConverted := deserialized.(AbsorbMessage)
	fmt.Println(msgConverted)
	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", deserialized)
}

func TestJoinReplyMsgSerializer(t *testing.T) {
	chain := PeerIDChain{}

	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	self := NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3}, 0, 0)

	children := []*PeerWithIDChain{
		NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3}, 0, 0),
		NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3}, 0, 0),
		NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3}, 0, 0),
	}

	toSerialize := NewJoinReplyMessage(children, self, self)
	serializer := toSerialize.Serializer()
	deserializer := toSerialize.Deserializer()
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := deserializer.Deserialize(msgBytes)
	msgConverted := deserialized.(JoinReplyMessage)
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

	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	sender := NewPeerWithIDChain(
		chain,
		peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300),
		3,
		0,
		Coordinates{3, 3, 3, 4},
		0, 0)

	sample := []*PeerWithIDChain{
		NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 1, 1, 2}, 0, 0),
		NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 3, 4, 1}, 0, 0),
		NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 0, 0, 0}, 0, 0),
	}
	toSerialize := NewRandomWalkMessage(3, sender, sample)

	serializer := toSerialize.Serializer()
	deserializer := toSerialize.Deserializer()

	msgBytes := serializer.Serialize(toSerialize)
	deserialized := deserializer.Deserialize(msgBytes)

	msgConverted := deserialized.(RandomWalkMessage)
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
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1}, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	sample := []*PeerWithIDChain{
		NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 1, 1, 2}, 0, 0),
		NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{3, 3, 4, 1}, 0, 0),
		NewPeerWithIDChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 3, 0, Coordinates{0, 0, 0, 0}, 0, 0),
	}
	toSerialize := NewWalkReplyMessage(sample)

	serializer := toSerialize.Serializer()
	deserializer := toSerialize.Deserializer()
	msgBytes := serializer.Serialize(toSerialize)
	deserialized := deserializer.Deserialize(msgBytes)

	msgConverted := deserialized.(WalkReplyMessage)
	fmt.Println(msgConverted)

	i := 0

	for _, p := range sample {
		t.Log(p.StringWithFields())
		t.Log(msgConverted.Sample[i].StringWithFields())
		i++
	}

	t.FailNow()
}
