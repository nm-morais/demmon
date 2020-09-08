package membership

import (
	"fmt"
	"net"
	"testing"

	"github.com/nm-morais/go-babel/pkg/peer"
)

func TestSerializePeerWithIDArray(t *testing.T) {
	toSerialize := NewPeerWithId([IdSegmentLen]byte{0, 1, 0, 0, 0, 0, 1}, peer.NewPeer(net.IPv4(10, 10, 0, 17), 1200, 1300))
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
