package membership

import (
	"encoding/binary"
	"time"

	"github.com/nm-morais/go-babel/pkg/message"
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
	Sender   *PeerWithIdChain
	Children []*PeerWithIdChain
	Level    uint16
}

func NewJoinReplyMessage(children []*PeerWithIdChain, level uint16, sender *PeerWithIdChain) joinReplyMessage {
	return joinReplyMessage{
		Children: children,
		Level:    level,
		Sender:   sender,
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
	msgBytes := make([]byte, 2)
	bufPos := 0
	binary.BigEndian.PutUint16(msgBytes[bufPos:bufPos+2], jrMsg.Level)
	msgBytes = append(msgBytes, jrMsg.Sender.MarshalWithFields()...)
	msgBytes = append(msgBytes, SerializePeerWithIDChainArray(jrMsg.Children)...)
	return msgBytes
}

func (JoinReplyMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
	bufPos += 2
	n, peer := UnmarshalPeerWithIdChain(msgBytes[bufPos:])
	bufPos += n
	_, hosts := DeserializePeerWithIDChainArray(msgBytes[bufPos:])
	return joinReplyMessage{Children: hosts, Level: level, Sender: peer}
}

// -------------- Update parent --------------

const updateParentMessageID = 1002

type updateParentMessage struct {
	GrandParent *PeerWithIdChain
	Parent      *PeerWithIdChain
	ProposedId  PeerID
	Siblings    []*PeerWithIdChain
}

func NewUpdateParentMessage(gparent *PeerWithIdChain, parent *PeerWithIdChain, proposedId PeerID, siblings []*PeerWithIdChain) updateParentMessage {

	upMsg := updateParentMessage{
		Parent:      parent,
		ProposedId:  proposedId,
		Siblings:    siblings,
		GrandParent: gparent,
	}

	return upMsg
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
	msgBytes := make([]byte, 0)
	msgBytes = append(msgBytes, uPMsg.ProposedId[:]...)
	msgBytes = append(msgBytes, uPMsg.Parent.MarshalWithFields()...)
	msgBytes = append(msgBytes, SerializePeerWithIDChainArray(uPMsg.Siblings)...)
	if uPMsg.GrandParent != nil {
		msgBytes = append(msgBytes, 1)
		msgBytes = append(msgBytes, uPMsg.GrandParent.MarshalWithFields()...)
	} else {
		msgBytes = append(msgBytes, 0)
	}
	return msgBytes
}

func (UpdateParentMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0

	var proposedId PeerID
	for i := 0; i < IdSegmentLen; i++ {
		proposedId[i] = msgBytes[bufPos]
		bufPos++
	}

	n, parent := UnmarshalPeerWithIdChain(msgBytes[bufPos:])
	bufPos += n

	n, siblings := DeserializePeerWithIDChainArray(msgBytes[bufPos:])
	bufPos += n

	var gParentFinal *PeerWithIdChain
	if msgBytes[bufPos] == 1 {
		bufPos++
		_, p := UnmarshalPeerWithIdChain(msgBytes[bufPos:])
		gParentFinal = p
	}
	return updateParentMessage{GrandParent: gParentFinal, ProposedId: proposedId, Siblings: siblings, Parent: parent}
}

// UPDATE CHILD message

type updateChildMessage struct {
	Child    *PeerWithIdChain
	Siblings []*MeasuredPeer
}

type updateChildMessageSerializer struct{}

const updateChildMessageID = 1003

func (updateChildMessage) Type() message.ID {
	return updateChildMessageID
}

func NewUpdateChildMessage(self *PeerWithIdChain, siblingLatencies MeasuredPeersByLat) updateChildMessage {

	return updateChildMessage{
		Child:    self,
		Siblings: siblingLatencies,
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
	var msgBytes []byte
	msgBytes = append(msgBytes, SerializeMeasuredPeerArray(ucMsg.Siblings)...)
	msgBytes = append(msgBytes, ucMsg.Child.MarshalWithFields()...)
	return msgBytes
}

func (updateChildMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	n, measuredPeers := DeserializeMeasuredPeerArray(msgBytes)
	bufPos += n
	_, child := UnmarshalPeerWithIdChain(msgBytes[bufPos:])
	return updateChildMessage{
		Child:    child,
		Siblings: measuredPeers,
	}
}

var updateChildMsgSerializer = updateChildMessageSerializer{}

// -------------- JoinAsParent --------------

const joinAsParentMessageID = 1004

type joinAsParentMessage struct {
	ExpectedId PeerIDChain
	ProposedId PeerIDChain
	Siblings   []*PeerWithIdChain
	Level      uint16
}

func NewJoinAsParentMessage(expectedId PeerIDChain, proposedId PeerIDChain, level uint16, siblings []*PeerWithIdChain) joinAsParentMessage {
	return joinAsParentMessage{
		ExpectedId: expectedId,
		Level:      level,
		ProposedId: proposedId,
		Siblings:   siblings,
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
	toSend := make([]byte, 2)
	binary.BigEndian.PutUint16(toSend[:2], japMsg.Level)
	toSend = append(toSend, SerializePeerIDChain(japMsg.ExpectedId)...)
	idBytes := SerializePeerIDChain(japMsg.ProposedId)
	toSend = append(toSend, idBytes...)
	toSend = append(toSend, SerializePeerWithIDChainArray(japMsg.Siblings)...)
	return toSend
}

func (JoinAsParentMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[0:2])
	bufPos += 2
	idSize, expectedId := DeserializePeerIDChain(msgBytes[bufPos:])
	bufPos += idSize
	n, proposedId := DeserializePeerIDChain(msgBytes[bufPos:])
	bufPos += n
	_, siblings := DeserializePeerWithIDChainArray(msgBytes[bufPos:])
	return joinAsParentMessage{
		Level:      level,
		ExpectedId: expectedId,
		ProposedId: proposedId,
		Siblings:   siblings,
	}
}

// -------------- Join As Child --------------

const joinAsChildMessageID = 1005

type joinAsChildMessage struct {
	Urgent          bool
	ExpectedId      PeerIDChain
	MeasuredLatency time.Duration
	Sender          *PeerWithIdChain
}

func NewJoinAsChildMessage(sender *PeerWithIdChain, measuredLatency time.Duration, expectedId PeerIDChain, urgent bool) joinAsChildMessage {
	return joinAsChildMessage{
		Urgent:          urgent,
		ExpectedId:      expectedId,
		MeasuredLatency: measuredLatency,
		Sender:          sender,
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

	msgBytes = append(msgBytes, jacMsg.Sender.MarshalWithFields()...)
	msgBytes = append(msgBytes, SerializePeerIDChain(jacMsg.ExpectedId)...)
	if jacMsg.Urgent {
		msgBytes = append(msgBytes, 1)
	} else {
		msgBytes = append(msgBytes, 0)
	}
	return msgBytes
}

func (JoinAsChildMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	measuredLatency := time.Duration(binary.BigEndian.Uint64(msgBytes[bufPos:]))
	bufPos += 8
	n, sender := UnmarshalPeerWithIdChain(msgBytes[bufPos:])
	bufPos += n
	n, peerIdChain := DeserializePeerIDChain(msgBytes[bufPos:])
	bufPos += n
	urgent := msgBytes[bufPos] == 1
	return joinAsChildMessage{MeasuredLatency: measuredLatency, ExpectedId: peerIdChain, Urgent: urgent, Sender: sender}
}

const joinAsChildMessageReplyID = 1006

type joinAsChildMessageReply struct {
	ParentLevel uint16
	ProposedId  PeerID
	Parent      *PeerWithIdChain
	GrandParent *PeerWithIdChain
	Siblings    []*PeerWithIdChain
	Accepted    bool
}

func NewJoinAsChildMessageReply(accepted bool, proposedId PeerID, level uint16, parent *PeerWithIdChain, siblings []*PeerWithIdChain, grandparent *PeerWithIdChain) joinAsChildMessageReply {
	jacMsg := joinAsChildMessageReply{
		Parent:      parent,
		ProposedId:  proposedId,
		Accepted:    accepted,
		ParentLevel: level,
		Siblings:    siblings,
		GrandParent: grandparent,
	}
	return jacMsg
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
	msgBytes = append(msgBytes, jacMsgR.ProposedId[:]...)
	msgBytes = append(msgBytes, jacMsgR.Parent.MarshalWithFields()...)
	msgBytes = append(msgBytes, SerializePeerWithIDChainArray(jacMsgR.Siblings)...)
	if jacMsgR.GrandParent == nil {
		msgBytes = append(msgBytes, 0)
		return msgBytes
	}
	msgBytes = append(msgBytes, 1)
	msgBytes = append(msgBytes, jacMsgR.GrandParent.MarshalWithFields()...)
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
	var proposedId PeerID
	for i := 0; i < IdSegmentLen; i++ {
		proposedId[i] = msgBytes[bufPos]
		bufPos++
	}
	n, parent := UnmarshalPeerWithIdChain(msgBytes[bufPos:])
	bufPos += n
	n, siblings := DeserializePeerWithIDChainArray(msgBytes[bufPos:])
	bufPos += n
	if msgBytes[bufPos] == 0 {
		return joinAsChildMessageReply{
			Parent:      parent,
			ProposedId:  proposedId,
			Accepted:    accepted,
			ParentLevel: level,
			Siblings:    siblings,
		}
	}
	bufPos++
	_, p := UnmarshalPeerWithIdChain(msgBytes[bufPos:])
	return joinAsChildMessageReply{
		Parent:      parent,
		ProposedId:  proposedId,
		Accepted:    accepted,
		ParentLevel: level,
		Siblings:    siblings,
		GrandParent: p,
	}
}

// ABSORB message

const absorbMessageID = 1007

type absorbMessage struct {
	PeerAbsorber  *PeerWithIdChain
	PeersToAbsorb []*PeerWithIdChain
}

func NewAbsorbMessage2(peersToAbsorb []*PeerWithIdChain, peerAbsorber *PeerWithIdChain) absorbMessage {

	return absorbMessage{
		PeersToAbsorb: peersToAbsorb,
		PeerAbsorber:  peerAbsorber,
	}
}

func NewAbsorbMessage(peersToAbsorb MeasuredPeersByLat, peerAbsorber *PeerWithIdChain) absorbMessage {

	peersToAbsorbAux := make([]*PeerWithIdChain, 0, len(peersToAbsorb))
	for _, p := range peersToAbsorb {
		peersToAbsorbAux = append(peersToAbsorbAux, p.PeerWithIdChain)
	}

	return absorbMessage{
		PeersToAbsorb: peersToAbsorbAux,
		PeerAbsorber:  peerAbsorber,
	}
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

type AbsorbMessageSerializer struct {
}

var absorbMessageSerializer = AbsorbMessageSerializer{}

func (AbsorbMessageSerializer) Serialize(msg message.Message) []byte {
	absMsg := msg.(absorbMessage)
	msgBytes := []byte{}
	msgBytes = append(msgBytes, absMsg.PeerAbsorber.MarshalWithFields()...)
	msgBytes = append(msgBytes, SerializePeerWithIDChainArray(absMsg.PeersToAbsorb)...)
	return msgBytes
}

func (AbsorbMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	n, p := UnmarshalPeerWithIdChain(msgBytes)
	_, peers := DeserializePeerWithIDChainArray(msgBytes[n:])
	return absorbMessage{PeersToAbsorb: peers, PeerAbsorber: p}
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

func NewRandomWalkMessage(ttl uint16, sender *PeerWithIdChain, sample []*PeerWithIdChain) randomWalkMessage {
	return randomWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

type randomWalkMessage struct {
	TTL    uint16
	Sender *PeerWithIdChain
	Sample []*PeerWithIdChain
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
	if len(randomWalk.Sample) == 0 {
		panic("sample len is 0")
	}
	msgBytes = append(msgBytes, SerializePeerWithIDChainArray(randomWalk.Sample)...)
	msgBytes = append(msgBytes, randomWalk.Sender.MarshalWithFields()...)
	return msgBytes
}

func (RandomWalkMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	ttl := binary.BigEndian.Uint16(msgBytes[0:2])
	n, sample := DeserializePeerWithIDChainArray(msgBytes[2:])
	_, sender := UnmarshalPeerWithIdChain(msgBytes[n+2:])
	return randomWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

// Biased walk

const biasedWalkMessageID = 1010

func NewBiasedWalkMessage(ttl uint16, sender *PeerWithIdChain, sample []*PeerWithIdChain) biasedWalkMessage {
	return biasedWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

type biasedWalkMessage struct {
	TTL    uint16
	Sender *PeerWithIdChain
	Sample []*PeerWithIdChain
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
	msgBytes = append(msgBytes, biasedWalk.Sender.MarshalWithFields()...)
	return msgBytes
}

func (BiasedWalkMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	ttl := binary.BigEndian.Uint16(msgBytes[0:2])
	n, sample := DeserializePeerWithIDChainArray(msgBytes[2:])
	_, sender := UnmarshalPeerWithIdChain(msgBytes[n+2:])
	return biasedWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

// Walk Reply

const walkReplyMessageID = 1011

func NewWalkReplyMessage(sample []*PeerWithIdChain) walkReplyMessage {
	return walkReplyMessage{
		Sample: sample,
	}
}

type walkReplyMessage struct {
	Sample []*PeerWithIdChain
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

const switchMessageID = 1012

func NewSwitchMessage(parent, grandparent *PeerWithIdChain, newChildren []*PeerWithIdChain, connectAsChild, connectAsParent bool) switchMessage {
	return switchMessage{
		Parent:          parent,
		ConnectAsChild:  connectAsChild,
		ConnectAsParent: connectAsParent,
	}
}

type switchMessage struct {
	GrandParent *PeerWithIdChain
	Children    []*PeerWithIdChain

	Parent          *PeerWithIdChain
	ConnectAsChild  bool
	ConnectAsParent bool
}

type SwitchMessageSerializer struct{}

func (switchMessage) Type() message.ID {
	return switchMessageID
}

var switchMessageSerializer = SwitchMessageSerializer{}

func (switchMessage) Serializer() message.Serializer {
	return switchMessageSerializer
}

func (switchMessage) Deserializer() message.Deserializer {
	return switchMessageSerializer
}

func (SwitchMessageSerializer) Serialize(msg message.Message) []byte {
	switchMsg := msg.(switchMessage)
	var msgBytes []byte
	msgBytes = append(msgBytes, switchMsg.Parent.MarshalWithFields()...)

	if switchMsg.GrandParent != nil {
		msgBytes = append(msgBytes, 1)
		msgBytes = append(msgBytes, switchMsg.GrandParent.MarshalWithFields()...)
	} else {
		msgBytes = append(msgBytes, 0)
	}

	msgBytes = append(msgBytes, SerializePeerWithIDChainArray(switchMsg.Children)...)
	if switchMsg.ConnectAsChild {
		msgBytes = append(msgBytes, 1)
	} else {
		msgBytes = append(msgBytes, 0)
	}
	if switchMsg.ConnectAsParent {
		msgBytes = append(msgBytes, 1)
	} else {
		msgBytes = append(msgBytes, 0)
	}

	return msgBytes
}

func (SwitchMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	n, parent := UnmarshalPeerWithIdChain(msgBytes[bufPos:])
	bufPos += n
	var gparent *PeerWithIdChain
	if msgBytes[bufPos] == 1 {
		n, gparent = UnmarshalPeerWithIdChain(msgBytes[bufPos:])
		bufPos += n
	} else {
		bufPos++
	}
	n, children := DeserializePeerWithIDChainArray(msgBytes[bufPos:])
	bufPos += n
	connectAsChild := msgBytes[bufPos] == 1
	bufPos += 1
	connectAsParent := msgBytes[bufPos] == 1
	return NewSwitchMessage(parent, gparent, children, connectAsChild, connectAsParent)
}
