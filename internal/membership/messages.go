package membership

import (
	"encoding/binary"
	"time"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
)

// -------------- Join --------------

const joinMessageID = 1000

type joinMessage struct {
}

func NewJoinMessage() joinMessage {
	return joinMessage{}
}

func (joinMessage) Type() message.ID {
	return joinMessageID
}

func (joinMessage) Serializer() message.Serializer {
	return joinMsgSerializer
}

func (joinMessage) Deserializer() message.Deserializer {
	return joinMsgSerializer
}

var joinMsgSerializer = JoinMsgSerializer{}

type JoinMsgSerializer struct{}

func (JoinMsgSerializer) Serialize(_ message.Message) []byte {
	return []byte{}
}

func (JoinMsgSerializer) Deserialize(_ []byte) message.Message {
	return joinMessage{}
}

// -------------- Join Reply --------------

const joinReplyMessageID = 1001

type joinReplyMessage struct {
	IdChain       PeerIDChain
	Children      []PeerWithId
	Level         uint16
	ParentLatency time.Duration
}

func NewJoinReplyMessage(children []PeerWithId, level uint16, parentLatency time.Duration, idChain PeerIDChain) joinReplyMessage {
	return joinReplyMessage{
		Children:      children,
		Level:         level,
		ParentLatency: parentLatency,
		IdChain:       idChain,
	}
}

func (joinReplyMessage) Type() message.ID {
	return joinReplyMessageID
}

func (joinReplyMessage) Serializer() message.Serializer {
	return joinReplyMsgSerializer
}

func (joinReplyMessage) Deserializer() message.Deserializer {
	return joinReplyMsgSerializer
}

type JoinReplyMsgSerializer struct{}

var joinReplyMsgSerializer = JoinReplyMsgSerializer{}

func (JoinReplyMsgSerializer) Serialize(msg message.Message) []byte {
	jrMsg := msg.(joinReplyMessage)
	msgBytes := make([]byte, 10)
	bufPos := 0
	binary.BigEndian.PutUint16(msgBytes[bufPos:bufPos+2], jrMsg.Level)
	bufPos += 2
	binary.BigEndian.PutUint64(msgBytes[bufPos:bufPos+8], uint64(jrMsg.ParentLatency))
	msgBytes = append(msgBytes, SerializePeerIDChain(jrMsg.IdChain)...)
	msgBytes = append(msgBytes, SerializePeerWithIDArray(jrMsg.Children)...)
	return msgBytes
}

func (JoinReplyMsgSerializer) Deserialize(msgBytes []byte) message.Message {

	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
	bufPos += 2
	parentLatency := time.Duration(binary.BigEndian.Uint64(msgBytes[bufPos : bufPos+8]))
	bufPos += 8
	n, peerIdChain := DeserializePeerIDChain(msgBytes[bufPos:])
	bufPos += n
	_, hosts := DeserializePeerWithIDArray(msgBytes[bufPos:])
	return joinReplyMessage{Children: hosts, Level: level, ParentLatency: parentLatency, IdChain: peerIdChain}
}

// -------------- Update parent --------------

const updateParentMessageID = 1002

type updateParentMessage struct {
	GrandParent     peer.Peer
	ProposedIdChain PeerIDChain
	ParentLevel     uint16
}

func NewUpdateParentMessage(gparent peer.Peer, parentLevel uint16, proposedIdChan PeerIDChain) updateParentMessage {
	return updateParentMessage{
		GrandParent:     gparent,
		ParentLevel:     parentLevel,
		ProposedIdChain: proposedIdChan,
	}
}

func (updateParentMessage) Type() message.ID {
	return updateParentMessageID
}

func (updateParentMessage) Serializer() message.Serializer {
	return updateParentMsgSerializer
}

func (updateParentMessage) Deserializer() message.Deserializer {
	return updateParentMsgSerializer
}

type UpdateParentMsgSerializer struct{}

var updateParentMsgSerializer = UpdateParentMsgSerializer{}

func (UpdateParentMsgSerializer) Serialize(msg message.Message) []byte {
	uPMsg := msg.(updateParentMessage)
	bufPos := 0
	msgBytes := make([]byte, 3)
	binary.BigEndian.PutUint16(msgBytes[bufPos:bufPos+2], uPMsg.ParentLevel)
	bufPos += 2
	if uPMsg.GrandParent != nil {
		msgBytes[bufPos] = 1
		gParentBytes := uPMsg.GrandParent.SerializeToBinary()
		msgBytes = append(msgBytes, gParentBytes...)
	} else {
		msgBytes[bufPos] = 0
	}
	msgBytes = append(msgBytes, SerializePeerIDChain(uPMsg.ProposedIdChain)...)
	return msgBytes
}

func (UpdateParentMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
	bufPos += 2
	var gParentFinal peer.Peer
	if msgBytes[bufPos] == 1 {
		bufPos++
		n, gParent := peer.DeserializePeer(msgBytes[bufPos:])
		gParentFinal = gParent
		bufPos += n
	} else {
		bufPos++
	}

	_, proposedId := DeserializePeerIDChain(msgBytes[bufPos:])
	return updateParentMessage{GrandParent: gParentFinal, ParentLevel: level, ProposedIdChain: proposedId}
}

// UPDATE CHILD message

type updateChildMessage struct {
	NChildren uint16
}

type updateChildMessageSerializer struct{}

const updateChildMessageID = 1003

func (updateChildMessage) Type() message.ID {
	return updateChildMessageID
}

func NewUpdateChildMessage(nChildren int) updateChildMessage {
	return updateChildMessage{
		NChildren: uint16(nChildren),
	}
}

func (updateChildMessage) Serializer() message.Serializer {
	return updateChildMsgSerializer
}

func (updateChildMessage) Deserializer() message.Deserializer {
	return updateChildMsgSerializer
}

func (updateChildMessageSerializer) Serialize(msg message.Message) []byte {
	ucMsg := msg.(updateChildMessage)
	nrChildrenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(nrChildrenBytes, ucMsg.NChildren)
	return nrChildrenBytes
}

func (updateChildMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	return updateChildMessage{
		NChildren: binary.BigEndian.Uint16(msgBytes[0:2]),
	}
}

var updateChildMsgSerializer = updateChildMessageSerializer{}

// -------------- JoinAsParent --------------

const joinAsParentMessageID = 1004

type joinAsParentMessage struct {
	ExpectedId      PeerIDChain
	ProposedId      PeerIDChain
	Level           uint16
	MeasuredLatency time.Duration
}

func NewJoinAsParentMessage(expectedId PeerIDChain, proposedId PeerIDChain, level uint16, measuredLatency time.Duration) joinAsParentMessage {
	return joinAsParentMessage{
		ExpectedId:      expectedId,
		Level:           level,
		MeasuredLatency: measuredLatency,
		ProposedId:      proposedId,
	}
}

func (joinAsParentMessage) Type() message.ID {
	return joinAsParentMessageID
}

func (joinAsParentMessage) Serializer() message.Serializer {
	return joinAsParentMsgSerializer
}

func (joinAsParentMessage) Deserializer() message.Deserializer {
	return joinAsParentMsgSerializer
}

type JoinAsParentMsgSerializer struct{}

var joinAsParentMsgSerializer = JoinAsParentMsgSerializer{}

func (JoinAsParentMsgSerializer) Serialize(msg message.Message) []byte {
	japMsg := msg.(joinAsParentMessage)
	toSend := make([]byte, 10)
	binary.BigEndian.PutUint16(toSend[:2], japMsg.Level)
	binary.BigEndian.PutUint64(toSend[2:10], uint64(japMsg.MeasuredLatency))
	toSend = append(toSend, SerializePeerIDChain(japMsg.ExpectedId)...)
	idBytes := SerializePeerIDChain(japMsg.ProposedId)
	toSend = append(toSend, idBytes...)
	return toSend
}

func (JoinAsParentMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[0:2])
	bufPos += 2
	measuredLatency := time.Duration(binary.BigEndian.Uint64(msgBytes[2:10]))
	bufPos += 8
	idSize, expectedId := DeserializePeerIDChain(msgBytes[bufPos:])
	bufPos += idSize
	_, proposedId := DeserializePeerIDChain(msgBytes[bufPos:])
	return joinAsParentMessage{
		MeasuredLatency: measuredLatency,
		Level:           level,
		ExpectedId:      expectedId,
		ProposedId:      proposedId,
	}
}

// -------------- Join As Child --------------

const joinAsChildMessageID = 1005

type joinAsChildMessage struct {
	Children       []peer.Peer
	ExpectedParent peer.Peer

	ExpectedId      PeerIDChain
	MeasuredLatency time.Duration
}

func NewJoinAsChildMessage(children []peer.Peer, measuredLatency time.Duration, expectedId PeerIDChain) joinAsChildMessage {
	return joinAsChildMessage{
		ExpectedId:      expectedId,
		Children:        children,
		MeasuredLatency: measuredLatency,
	}
}

func (joinAsChildMessage) Type() message.ID {
	return joinAsChildMessageID
}

func (joinAsChildMessage) Serializer() message.Serializer {
	return joinAsChildMsgSerializer
}

func (joinAsChildMessage) Deserializer() message.Deserializer {
	return joinAsChildMsgSerializer
}

type JoinAsChildMsgSerializer struct{}

var joinAsChildMsgSerializer = JoinAsChildMsgSerializer{}

func (JoinAsChildMsgSerializer) Serialize(msg message.Message) []byte {
	jacMsg := msg.(joinAsChildMessage)
	msgBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(msgBytes, uint64(jacMsg.MeasuredLatency))
	msgBytes = append(msgBytes, peer.SerializePeerArray(msg.(joinAsChildMessage).Children)...)
	msgBytes = append(msgBytes, SerializePeerIDChain(jacMsg.ExpectedId)...)
	return msgBytes
}

func (JoinAsChildMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	measuredLatency := time.Duration(binary.BigEndian.Uint64(msgBytes[bufPos:8]))
	bufPos += 8
	n, children := peer.DeserializePeerArray(msgBytes[bufPos:])
	bufPos += n
	_, peerIdChain := DeserializePeerIDChain(msgBytes[bufPos:])
	return joinAsChildMessage{Children: children, MeasuredLatency: measuredLatency, ExpectedId: peerIdChain}
}

const joinAsChildMessageReplyID = 1006

type joinAsChildMessageReply struct {
	ProposedId  PeerIDChain
	ParentLevel uint16
	Siblings    []PeerWithId
	Accepted    bool
}

func NewJoinAsChildMessageReply(accepted bool, proposedId PeerIDChain, level uint16, siblings []PeerWithId) joinAsChildMessageReply {
	return joinAsChildMessageReply{
		ProposedId:  proposedId,
		Accepted:    accepted,
		ParentLevel: level,
		Siblings:    siblings,
	}
}

func (joinAsChildMessageReply) Type() message.ID {
	return joinAsChildMessageReplyID
}

func (joinAsChildMessageReply) Serializer() message.Serializer {
	return joinAsChildMessageReplySerializer
}

func (joinAsChildMessageReply) Deserializer() message.Deserializer {
	return joinAsChildMessageReplySerializer
}

type JoinAsChildMessageReplySerializer struct{}

var joinAsChildMessageReplySerializer = JoinAsChildMessageReplySerializer{}

func (JoinAsChildMessageReplySerializer) Serialize(msg message.Message) []byte {
	jacMsgR := msg.(joinAsChildMessageReply)
	if !jacMsgR.Accepted {
		return []byte{0}
	}
	msgBytes := make([]byte, 3)
	bufPos := 0
	msgBytes[bufPos] = 1
	bufPos++
	binary.BigEndian.PutUint16(msgBytes[bufPos:], jacMsgR.ParentLevel)
	msgBytes = append(msgBytes, SerializePeerIDChain(jacMsgR.ProposedId)...)
	msgBytes = append(msgBytes, SerializePeerWithIDArray(jacMsgR.Siblings)...)
	return msgBytes
}

func (JoinAsChildMessageReplySerializer) Deserialize(msgBytes []byte) message.Message {
	accepted := msgBytes[0] == 1
	if !accepted {
		return joinAsChildMessageReply{Accepted: false}
	}

	bufPos := 1
	level := binary.BigEndian.Uint16(msgBytes[bufPos:])
	bufPos += 2
	n, proposedId := DeserializePeerIDChain(msgBytes[bufPos:])
	bufPos += n
	_, siblings := DeserializePeerWithIDArray(msgBytes[bufPos:])

	return joinAsChildMessageReply{
		ProposedId:  proposedId,
		Accepted:    accepted,
		ParentLevel: level,
		Siblings:    siblings,
	}
}

// ABSORB message

const absorbMessageID = 1007

type absorbMessage struct {
}

func NewAbsorbMessage() absorbMessage {
	return absorbMessage{}
}

func (absorbMessage) Type() message.ID {
	return absorbMessageID
}

func (absorbMessage) Serializer() message.Serializer {
	return absorbMessageSerializer
}

func (absorbMessage) Deserializer() message.Deserializer {
	return absorbMessageSerializer
}

type AbsorbMessageSerializer struct{}

var absorbMessageSerializer = AbsorbMessageSerializer{}

func (AbsorbMessageSerializer) Serialize(msg message.Message) []byte {
	return []byte{}
}

func (AbsorbMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	return absorbMessage{}
}

// DISCONNECT AS CHILD message

const disconnectAsChildMessageID = 1008

func NewDisconnectAsChildMessage() disconnectAsChildMessage {
	return disconnectAsChildMessage{}
}

type disconnectAsChildMessage struct{}

type DisconnectAsChildMsgSerializer struct{}

func (disconnectAsChildMessage) Type() message.ID {
	return disconnectAsChildMessageID
}

var disconnectAsChildMsgSerializer = DisconnectAsChildMsgSerializer{}

func (disconnectAsChildMessage) Serializer() message.Serializer {
	return disconnectAsChildMsgSerializer
}

func (disconnectAsChildMessage) Deserializer() message.Deserializer {
	return disconnectAsChildMsgSerializer
}

func (DisconnectAsChildMsgSerializer) Serialize(msg message.Message) []byte {
	return []byte{}
}

func (DisconnectAsChildMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	return disconnectAsChildMessage{}
}

// Random walk

const randomWalkMessageID = 1009

func NewRandomWalkMessage(ttl uint16, sender PeerWithIdChain, sample []PeerWithIdChain) randomWalkMessage {
	return randomWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

type randomWalkMessage struct {
	TTL    uint16
	Sender PeerWithIdChain
	Sample []PeerWithIdChain
}

type RandomWalkMessageSerializer struct{}

func (randomWalkMessage) Type() message.ID {
	return randomWalkMessageID
}

var randomWalkMessageSerializer = RandomWalkMessageSerializer{}

func (randomWalkMessage) Serializer() message.Serializer {
	return randomWalkMessageSerializer
}

func (randomWalkMessage) Deserializer() message.Deserializer {
	return randomWalkMessageSerializer
}

func (RandomWalkMessageSerializer) Serialize(msg message.Message) []byte {
	var msgBytes []byte
	ttlBytes := make([]byte, 2)
	randomWalk := msg.(randomWalkMessage)
	binary.BigEndian.PutUint16(ttlBytes[0:2], randomWalk.TTL)
	msgBytes = ttlBytes
	msgBytes = append(msgBytes, SerializePeerWithIDChainArray(randomWalk.Sample)...)
	msgBytes = append(msgBytes, randomWalk.Sender.SerializeToBinary()...)
	return msgBytes
}

func (RandomWalkMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	ttl := binary.BigEndian.Uint16(msgBytes[0:2])
	n, sample := DeserializePeerWithIDChainArray(msgBytes[2:])
	_, sender := DeserializePeerWithIdChain(msgBytes[n+2:])
	return randomWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

// Biased walk

const biasedWalkMessageID = 1010

func NewBiasedWalkMessage(ttl uint16, sender PeerWithIdChain, sample []PeerWithIdChain) biasedWalkMessage {
	return biasedWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

type biasedWalkMessage struct {
	TTL    uint16
	Sender PeerWithIdChain
	Sample []PeerWithIdChain
}

type BiasedWalkMessageSerializer struct{}

func (biasedWalkMessage) Type() message.ID {
	return biasedWalkMessageID
}

var biasedWalkMessageSerializer = BiasedWalkMessageSerializer{}

func (biasedWalkMessage) Serializer() message.Serializer {
	return biasedWalkMessageSerializer
}

func (biasedWalkMessage) Deserializer() message.Deserializer {
	return biasedWalkMessageSerializer
}

func (BiasedWalkMessageSerializer) Serialize(msg message.Message) []byte {
	biasedWalk := msg.(biasedWalkMessage)
	var msgBytes []byte
	ttlBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(ttlBytes[0:2], biasedWalk.TTL)
	msgBytes = ttlBytes
	msgBytes = append(msgBytes, SerializePeerWithIDChainArray(biasedWalk.Sample)...)
	msgBytes = append(msgBytes, biasedWalk.Sender.SerializeToBinary()...)
	return msgBytes
}

func (BiasedWalkMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	ttl := binary.BigEndian.Uint16(msgBytes[0:2])
	n, sample := DeserializePeerWithIDChainArray(msgBytes[2:])
	_, sender := DeserializePeerWithIdChain(msgBytes[n+2:])
	return biasedWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

// Walk Reply

const walkReplyMessageID = 1011

func NewWalkReplyMessage(sample []PeerWithIdChain) walkReplyMessage {
	return walkReplyMessage{
		Sample: sample,
	}
}

type walkReplyMessage struct {
	Sample []PeerWithIdChain
}

type WalkReplyMessageSerializer struct{}

func (walkReplyMessage) Type() message.ID {
	return walkReplyMessageID
}

var walkReplyMessageSerializer = WalkReplyMessageSerializer{}

func (walkReplyMessage) Serializer() message.Serializer {
	return walkReplyMessageSerializer
}

func (walkReplyMessage) Deserializer() message.Deserializer {
	return walkReplyMessageSerializer
}

func (WalkReplyMessageSerializer) Serialize(msg message.Message) []byte {
	walkReply := msg.(walkReplyMessage)
	return SerializePeerWithIDChainArray(walkReply.Sample)
}

func (WalkReplyMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	_, sample := DeserializePeerWithIDChainArray(msgBytes)
	return walkReplyMessage{Sample: sample}
}
