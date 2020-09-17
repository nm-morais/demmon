package membership

import (
	"fmt"
	"net"
	"testing"

	"github.com/nm-morais/go-babel/pkg/peer"
)

func TestSerializePeerWithID(t *testing.T) {
	toSerialize := NewPeerWithId(PeerID{1, 0}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 0)
	PwIDbytes := toSerialize.SerializeToBinary()
	_, deserialized := DeserializePeerWithId(PwIDbytes)
	t.Logf("%+v,", toSerialize)
	t.Logf("%+v,", deserialized)
	fmt.Printf("%+v\n", toSerialize)
	fmt.Printf("%+v\n", deserialized)
	for i := 0; i < len(toSerialize.ID()); i++ {
		if toSerialize.ID()[i] != deserialized.ID()[i] {
			t.Logf("%+v,", toSerialize.ID())
			t.Logf("%+v,", deserialized.ID())
			t.FailNow()
		}
	}

	if !toSerialize.Peer().Equals(deserialized.Peer()) {
		t.FailNow()
	}
	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", deserialized)
}

func TestSerializePeerWithIDArray(t *testing.T) {
	toSerialize := []PeerWithId{
		NewPeerWithId(PeerID{1, 1, 1, 1, 1, 0}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 0),
		NewPeerWithId(PeerID{1, 1, 1, 1, 1, 1, 1, 1}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 0),
		NewPeerWithId(PeerID{29, 0}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 0),
	}
	serialized := SerializePeerWithIDArray(toSerialize)
	_, deserialized := DeserializePeerWithIDArray(serialized)
	t.Logf("%+v,", toSerialize)
	t.Logf("%+v,", deserialized)
	for i := 0; i < len(toSerialize); i++ {
		curr := toSerialize[i]
		curr2 := deserialized[i]
		t.Logf("%+v,", curr)
		t.Logf("%+v,", curr2)
		for j := 0; j < len(curr.ID()); j++ {
			if curr.ID()[j] != curr2.ID()[j] {
				t.Logf("%+v,", curr)
				t.Logf("%+v,", curr2)
				t.FailNow()
			}
		}

		if !curr.Peer().Equals(curr2.Peer()) {
			t.FailNow()
		}
	}
	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", deserialized)
}

func TestIsDescendant(t *testing.T) {

	parentChain := PeerIDChain{}
	parentChain = append(parentChain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	parentChain = append(parentChain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	childChain := append(parentChain, PeerID{1, 1, 0, 1, 1, 0, 0, 0})

	if !IsDescendant(parentChain, childChain) {
		t.FailNow()
	}
}
