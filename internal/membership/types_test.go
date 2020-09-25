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

	if !toSerialize.Equals(deserialized) {
		t.FailNow()
	}
	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", deserialized)
}

func TestSerializePeerWithIDChain(t *testing.T) {
	chain := PeerIDChain{}
	chain = append(chain, PeerID{0, 0, 0, 1, 1, 0, 0, 1})
	chain = append(chain, PeerID{1, 1, 0, 1, 1, 0, 0, 1})

	toSerialize := NewPeerWithIdChain(chain, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300), 200)
	PwIDbytes := toSerialize.SerializeToBinary()
	_, deserialized := DeserializePeerWithIdChain(PwIDbytes)
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

	if !toSerialize.Equals(deserialized) {
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

		if !curr.Equals(curr2) {
			t.FailNow()
		}
	}
	t.Logf("before: %+v", toSerialize)
	t.Logf("after: %+v", deserialized)
}
