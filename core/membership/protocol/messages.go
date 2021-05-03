package protocol

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/sirupsen/logrus"
)

// -------------- Join --------------

const joinMessageID = 2000

type JoinMessage struct {
}

func NewJoinMessage() JoinMessage {
	return JoinMessage{}
}

func (JoinMessage) Type() message.ID {
	return joinMessageID
}

func (JoinMessage) Serializer() message.Serializer {
	return joinMsgSerializer
}

func (JoinMessage) Deserializer() message.Deserializer {
	return joinMsgSerializer
}

var joinMsgSerializer = JoinMsgSerializer{}

type JoinMsgSerializer struct{}

func (JoinMsgSerializer) Serialize(_ message.Message) []byte {
	return []byte{}
}

func (JoinMsgSerializer) Deserialize(_ []byte) message.Message {
	return JoinMessage{}
}

// -------------- Join Reply --------------

const joinReplyMessageID = 2001

type JoinReplyMessage struct {
	Parent   *PeerWithIDChain
	Sender   *PeerWithIDChain
	Children []*PeerWithIDChain
}

func NewJoinReplyMessage(children []*PeerWithIDChain, sender, parent *PeerWithIDChain) JoinReplyMessage {
	return JoinReplyMessage{
		Children: ClonePeerWithIDChainArr(children),
		Sender:   sender,
		Parent:   parent,
	}
}

func (JoinReplyMessage) Type() message.ID {
	return joinReplyMessageID
}

func (JoinReplyMessage) Serializer() message.Serializer {
	return joinReplyMsgSerializer
}

func (JoinReplyMessage) Deserializer() message.Deserializer {
	return joinReplyMsgSerializer
}

type JoinReplyMsgSerializer struct{}

var joinReplyMsgSerializer = JoinReplyMsgSerializer{}

func (JoinReplyMsgSerializer) Serialize(msg message.Message) []byte {
	jrMsg := msg.(JoinReplyMessage)

	var msgBytes []byte

	msgBytes = append(msgBytes, jrMsg.Sender.MarshalWithFields()...)
	if jrMsg.Parent != nil {
		msgBytes = append(msgBytes, 1)
		msgBytes = append(msgBytes, jrMsg.Parent.MarshalWithFields()...)
	} else {
		msgBytes = append(msgBytes, 0)
	}

	return append(msgBytes, SerializePeerWithIDChainArray(jrMsg.Children)...)
}

func (JoinReplyMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	n, sender := UnmarshalPeerWithIDChain(msgBytes[bufPos:])
	bufPos += n
	var parent *PeerWithIDChain

	if msgBytes[bufPos] == 1 {
		bufPos++
		n, parent = UnmarshalPeerWithIDChain(msgBytes[bufPos:])
		bufPos += n
	} else {
		bufPos++
	}

	_, hosts := DeserializePeerWithIDChainArray(msgBytes[bufPos:])

	return JoinReplyMessage{Children: hosts, Sender: sender, Parent: parent}
}

// -------------- Update parent --------------

const UpdateParentMessageID = 2002

type UpdateParentMessage struct {
	GrandParent *PeerWithIDChain
	Parent      *PeerWithIDChain
	ProposedID  PeerID
	Siblings    []*PeerWithIDChain
}

func NewUpdateParentMessage(
	gparent, parent *PeerWithIDChain,
	proposedID PeerID,
	siblings []*PeerWithIDChain,
) UpdateParentMessage {
	upMsg := UpdateParentMessage{
		Parent:      parent.Clone(),
		ProposedID:  proposedID,
		Siblings:    ClonePeerWithIDChainArr(siblings),
		GrandParent: gparent.Clone(),
	}

	return upMsg
}

func (UpdateParentMessage) Type() message.ID {
	return UpdateParentMessageID
}

func (UpdateParentMessage) Serializer() message.Serializer {
	return updateParentMsgSerializer
}

func (UpdateParentMessage) Deserializer() message.Deserializer {
	return updateParentMsgSerializer
}

type UpdateParentMsgSerializer struct{}

var updateParentMsgSerializer = UpdateParentMsgSerializer{}

func (UpdateParentMsgSerializer) Serialize(msg message.Message) []byte {
	uPMsg := msg.(UpdateParentMessage)
	msgBytes := make([]byte, 0)
	msgBytes = append(msgBytes, uPMsg.ProposedID[:]...)
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

	var (
		proposedID   PeerID
		gParentFinal *PeerWithIDChain
	)

	for i := 0; i < IDSegmentLen; i++ {
		proposedID[i] = msgBytes[bufPos]
		bufPos++
	}

	n, parent := UnmarshalPeerWithIDChain(msgBytes[bufPos:])
	bufPos += n

	n, siblings := DeserializePeerWithIDChainArray(msgBytes[bufPos:])
	bufPos += n

	if msgBytes[bufPos] == 1 {
		bufPos++
		_, p := UnmarshalPeerWithIDChain(msgBytes[bufPos:])
		gParentFinal = p
	}

	return UpdateParentMessage{GrandParent: gParentFinal, ProposedID: proposedID, Siblings: siblings, Parent: parent}
}

// UPDATE CHILD message

type UpdateChildMessage struct {
	Child    *PeerWithIDChain
	Siblings []*MeasuredPeer
}

type updateChildMessageSerializer struct{}

const updateChildMessageID = 2003

func (UpdateChildMessage) Type() message.ID {
	return updateChildMessageID
}

func NewUpdateChildMessage(self *PeerWithIDChain, siblingLatencies MeasuredPeersByLat) UpdateChildMessage {
	return UpdateChildMessage{
		Child:    self.Clone(),
		Siblings: siblingLatencies,
	}
}

func (UpdateChildMessage) Serializer() message.Serializer {
	return updateChildMsgSerializer
}

func (UpdateChildMessage) Deserializer() message.Deserializer {
	return updateChildMsgSerializer
}

func (updateChildMessageSerializer) Serialize(msg message.Message) []byte {
	ucMsg := msg.(UpdateChildMessage)

	var msgBytes []byte
	msgBytes = append(msgBytes, SerializeMeasuredPeerArray(ucMsg.Siblings)...)
	msgBytes = append(msgBytes, ucMsg.Child.MarshalWithFields()...)

	return msgBytes
}

func (updateChildMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	n, measuredPeers := DeserializeMeasuredPeerArray(msgBytes)
	bufPos += n
	_, child := UnmarshalPeerWithIDChain(msgBytes[bufPos:])

	return UpdateChildMessage{
		Child:    child,
		Siblings: measuredPeers,
	}
}

var updateChildMsgSerializer = updateChildMessageSerializer{}

// -------------- JoinAsParent --------------

const joinAsParentMessageID = 2004

type JoinAsParentMessage struct {
	ExpectedID PeerIDChain
	ProposedID PeerIDChain
	Siblings   []*PeerWithIDChain
	Level      uint16
}

func NewJoinAsParentMessage(
	expectedID, proposedID PeerIDChain,
	level uint16,
	siblings []*PeerWithIDChain,
) JoinAsParentMessage {
	return JoinAsParentMessage{
		ExpectedID: expectedID,
		Level:      level,
		ProposedID: proposedID,
		Siblings:   siblings,
	}
}

func (JoinAsParentMessage) Type() message.ID {
	return joinAsParentMessageID
}

func (JoinAsParentMessage) Serializer() message.Serializer {
	return joinAsParentMsgSerializer
}

func (JoinAsParentMessage) Deserializer() message.Deserializer {
	return joinAsParentMsgSerializer
}

type JoinAsParentMsgSerializer struct{}

var joinAsParentMsgSerializer = JoinAsParentMsgSerializer{}

func (JoinAsParentMsgSerializer) Serialize(msg message.Message) []byte {
	japMsg := msg.(JoinAsParentMessage)
	toSend := make([]byte, 2)
	binary.BigEndian.PutUint16(toSend[:2], japMsg.Level)
	toSend = append(toSend, SerializePeerIDChain(japMsg.ExpectedID)...)
	idBytes := SerializePeerIDChain(japMsg.ProposedID)
	toSend = append(toSend, idBytes...)
	toSend = append(toSend, SerializePeerWithIDChainArray(japMsg.Siblings)...)

	return toSend
}

func (JoinAsParentMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[0:2])
	bufPos += 2
	idSize, expectedID := DeserializePeerIDChain(msgBytes[bufPos:])
	bufPos += idSize
	n, proposedID := DeserializePeerIDChain(msgBytes[bufPos:])
	bufPos += n
	_, siblings := DeserializePeerWithIDChainArray(msgBytes[bufPos:])

	return JoinAsParentMessage{
		Level:      level,
		ExpectedID: expectedID,
		ProposedID: proposedID,
		Siblings:   siblings,
	}
}

// -------------- Join As Child --------------

const joinAsChildMessageID = 2005

type JoinAsChildMessage struct {
	BWScore         int
	Improvement     bool
	Urgent          bool
	ExpectedID      PeerIDChain
	MeasuredLatency time.Duration
	Sender          *PeerWithIDChain
}

func NewJoinAsChildMessage(
	bwScore int,
	sender *PeerWithIDChain,
	measuredLatency time.Duration,
	expectedID PeerIDChain,
	urgent,
	improvement bool,
) JoinAsChildMessage {
	return JoinAsChildMessage{
		BWScore:         bwScore,
		Improvement:     improvement,
		Urgent:          urgent,
		ExpectedID:      expectedID,
		MeasuredLatency: measuredLatency,
		Sender:          sender.Clone(),
	}
}

func (JoinAsChildMessage) Type() message.ID {
	return joinAsChildMessageID
}

func (JoinAsChildMessage) Serializer() message.Serializer {
	return joinAsChildMsgSerializer
}

func (JoinAsChildMessage) Deserializer() message.Deserializer {
	return joinAsChildMsgSerializer
}

type JoinAsChildMsgSerializer struct{}

var joinAsChildMsgSerializer = JoinAsChildMsgSerializer{}

func (JoinAsChildMsgSerializer) Serialize(msg message.Message) []byte {
	jacMsg := msg.(JoinAsChildMessage)

	msgBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(msgBytes, uint64(jacMsg.MeasuredLatency))

	scoreBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(scoreBytes, uint32(jacMsg.BWScore))
	msgBytes = append(msgBytes, scoreBytes...)

	msgBytes = append(msgBytes, jacMsg.Sender.MarshalWithFields()...)
	msgBytes = append(msgBytes, SerializePeerIDChain(jacMsg.ExpectedID)...)

	if jacMsg.Urgent {
		msgBytes = append(msgBytes, 1)
	} else {
		msgBytes = append(msgBytes, 0)
	}

	if jacMsg.Improvement {
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
	bwScore := time.Duration(binary.BigEndian.Uint32(msgBytes[bufPos:]))
	bufPos += 4
	n, sender := UnmarshalPeerWithIDChain(msgBytes[bufPos:])
	bufPos += n
	n, peerIDChain := DeserializePeerIDChain(msgBytes[bufPos:])
	bufPos += n
	urgent := msgBytes[bufPos] == 1
	bufPos += 1
	improvement := msgBytes[bufPos] == 1

	return JoinAsChildMessage{
		BWScore:         int(bwScore),
		Improvement:     improvement,
		Urgent:          urgent,
		ExpectedID:      peerIDChain,
		MeasuredLatency: measuredLatency,
		Sender:          sender,
	}
}

const joinAsChildMessageReplyID = 2006

type JoinAsChildMessageReply struct {
	Siblings    []*PeerWithIDChain
	Parent      *PeerWithIDChain
	GrandParent *PeerWithIDChain
	ParentLevel uint16
	ProposedID  PeerID
	Accepted    bool
}

func NewJoinAsChildMessageReply(
	accepted bool,
	proposedID PeerID,
	level int,
	parent *PeerWithIDChain,
	siblings []*PeerWithIDChain,
	grandparent *PeerWithIDChain,
) JoinAsChildMessageReply {
	jacMsg := JoinAsChildMessageReply{
		Parent:      parent.Clone(),
		ProposedID:  proposedID,
		Accepted:    accepted,
		ParentLevel: uint16(level),
		Siblings:    ClonePeerWithIDChainArr(siblings),
		GrandParent: grandparent.Clone(),
	}

	return jacMsg
}

func (JoinAsChildMessageReply) Type() message.ID {
	return joinAsChildMessageReplyID
}

func (JoinAsChildMessageReply) Serializer() message.Serializer {
	return joinAsChildMessageReplySerializer
}

func (JoinAsChildMessageReply) Deserializer() message.Deserializer {
	return joinAsChildMessageReplySerializer
}

type JoinAsChildMessageReplySerializer struct{}

var joinAsChildMessageReplySerializer = JoinAsChildMessageReplySerializer{}

func (JoinAsChildMessageReplySerializer) Serialize(msg message.Message) []byte {
	jacMsgR := msg.(JoinAsChildMessageReply)
	msgBytes := make([]byte, 3)

	if jacMsgR.Accepted {
		msgBytes[0] = 1
	} else {
		msgBytes[0] = 0
	}

	bufPos := 1
	binary.BigEndian.PutUint16(msgBytes[bufPos:], jacMsgR.ParentLevel)
	msgBytes = append(msgBytes, jacMsgR.Parent.MarshalWithFields()...)

	if !jacMsgR.Accepted {
		return msgBytes
	}

	msgBytes = append(msgBytes, jacMsgR.ProposedID[:]...)

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
	bufPos := 1
	level := binary.BigEndian.Uint16(msgBytes[bufPos:])
	bufPos += 2

	n, parent := UnmarshalPeerWithIDChain(msgBytes[bufPos:])
	bufPos += n

	if !accepted {
		return JoinAsChildMessageReply{Accepted: accepted, Parent: parent, ParentLevel: level}
	}

	var proposedID PeerID
	for i := 0; i < IDSegmentLen; i++ {
		proposedID[i] = msgBytes[bufPos]

		bufPos++
	}

	n, siblings := DeserializePeerWithIDChainArray(msgBytes[bufPos:])

	bufPos += n

	if msgBytes[bufPos] == 0 {
		return JoinAsChildMessageReply{
			Parent:      parent,
			ProposedID:  proposedID,
			Accepted:    accepted,
			ParentLevel: level,
			Siblings:    siblings,
		}
	}

	bufPos++
	_, p := UnmarshalPeerWithIDChain(msgBytes[bufPos:])

	return JoinAsChildMessageReply{
		Parent:      parent,
		ProposedID:  proposedID,
		Accepted:    accepted,
		ParentLevel: level,
		Siblings:    siblings,
		GrandParent: p,
	}
}

// ABSORB message

const absorbMessageID = 2007

type AbsorbMessage struct {
	peerAbsorber *PeerWithIDChain
}

func NewAbsorbMessage(peerAbsorber *PeerWithIDChain) AbsorbMessage {
	return AbsorbMessage{
		peerAbsorber: peerAbsorber.Clone(),
	}
}

func (AbsorbMessage) Type() message.ID {
	return absorbMessageID
}

func (AbsorbMessage) Serializer() message.Serializer {
	return absorbMessageSerializer
}

func (AbsorbMessage) Deserializer() message.Deserializer {
	return absorbMessageSerializer
}

type AbsorbMessageSerializer struct {
}

var absorbMessageSerializer = AbsorbMessageSerializer{}

func (AbsorbMessageSerializer) Serialize(msg message.Message) []byte {
	absMsg := msg.(AbsorbMessage)
	return absMsg.peerAbsorber.MarshalWithFields()
}

func (AbsorbMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	_, peerAbsorber := UnmarshalPeerWithIDChain(msgBytes)
	return AbsorbMessage{peerAbsorber: peerAbsorber}
}

// DISCONNECT AS CHILD message

const disconnectAsChildMessageID = 2008

func NewDisconnectAsChildMessage() DisconnectAsChildMessage {
	return DisconnectAsChildMessage{}
}

type DisconnectAsChildMessage struct{}

type DisconnectAsChildMsgSerializer struct{}

func (DisconnectAsChildMessage) Type() message.ID {
	return disconnectAsChildMessageID
}

var disconnectAsChildMsgSerializer = DisconnectAsChildMsgSerializer{}

func (DisconnectAsChildMessage) Serializer() message.Serializer {
	return disconnectAsChildMsgSerializer
}

func (DisconnectAsChildMessage) Deserializer() message.Deserializer {
	return disconnectAsChildMsgSerializer
}

func (DisconnectAsChildMsgSerializer) Serialize(_ message.Message) []byte {
	return []byte{}
}

func (DisconnectAsChildMsgSerializer) Deserialize(_ []byte) message.Message {
	return DisconnectAsChildMessage{}
}

// Random walk

const randomWalkMessageID = 2009

func NewRandomWalkMessage(ttl uint16, sender *PeerWithIDChain, sample []*PeerWithIDChain) RandomWalkMessage {
	duplChecker := map[string]bool{}
	for _, elem := range sample {
		k := elem.String()
		if _, ok := duplChecker[k]; ok {
			logrus.Panicf("Duplicate entry in sample: %+v", sample)
		}
		duplChecker[k] = true
	}
	return RandomWalkMessage{
		TTL:    ttl,
		Sample: ClonePeerWithIDChainArr(sample),
		Sender: sender.Clone(),
	}
}

type RandomWalkMessage struct {
	TTL    uint16
	Sender *PeerWithIDChain
	Sample []*PeerWithIDChain
}

type RandomWalkMessageSerializer struct{}

func (RandomWalkMessage) Type() message.ID {
	return randomWalkMessageID
}

var randomWalkMessageSerializer = RandomWalkMessageSerializer{}

func (RandomWalkMessage) Serializer() message.Serializer {
	return randomWalkMessageSerializer
}

func (RandomWalkMessage) Deserializer() message.Deserializer {
	return randomWalkMessageSerializer
}

func (RandomWalkMessageSerializer) Serialize(msg message.Message) []byte {
	var msgBytes []byte

	ttlBytes := make([]byte, 2)
	randomWalk := msg.(RandomWalkMessage)
	binary.BigEndian.PutUint16(ttlBytes[0:2], randomWalk.TTL)
	msgBytes = ttlBytes

	msgBytes = append(msgBytes, SerializePeerWithIDChainArray(randomWalk.Sample)...)
	msgBytes = append(msgBytes, randomWalk.Sender.MarshalWithFields()...)

	return msgBytes
}

func (RandomWalkMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	ttl := binary.BigEndian.Uint16(msgBytes[0:2])
	n, sample := DeserializePeerWithIDChainArray(msgBytes[2:])
	_, sender := UnmarshalPeerWithIDChain(msgBytes[n+2:])

	return RandomWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

// Biased walk

const biasedWalkMessageID = 1010

func NewBiasedWalkMessage(ttl uint16, sender *PeerWithIDChain, sample []*PeerWithIDChain) BiasedWalkMessage {
	return BiasedWalkMessage{
		TTL:    ttl,
		Sample: ClonePeerWithIDChainArr(sample),
		Sender: sender.Clone(),
	}
}

type BiasedWalkMessage struct {
	TTL    uint16
	Sender *PeerWithIDChain
	Sample []*PeerWithIDChain
}

type BiasedWalkMessageSerializer struct{}

func (BiasedWalkMessage) Type() message.ID {
	return biasedWalkMessageID
}

var biasedWalkMessageSerializer = BiasedWalkMessageSerializer{}

func (BiasedWalkMessage) Serializer() message.Serializer {
	return biasedWalkMessageSerializer
}

func (BiasedWalkMessage) Deserializer() message.Deserializer {
	return biasedWalkMessageSerializer
}

func (BiasedWalkMessageSerializer) Serialize(msg message.Message) []byte {
	biasedWalk := msg.(BiasedWalkMessage)

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
	_, sender := UnmarshalPeerWithIDChain(msgBytes[n+2:])

	return BiasedWalkMessage{
		TTL:    ttl,
		Sample: sample,
		Sender: sender,
	}
}

// Walk Reply

const walkReplyMessageID = 1011

func NewWalkReplyMessage(sample []*PeerWithIDChain) WalkReplyMessage {
	return WalkReplyMessage{
		Sample: ClonePeerWithIDChainArr(sample),
	}
}

type WalkReplyMessage struct {
	Sample []*PeerWithIDChain
}

type WalkReplyMessageSerializer struct{}

func (WalkReplyMessage) Type() message.ID {
	return walkReplyMessageID
}

var walkReplyMessageSerializer = WalkReplyMessageSerializer{}

func (WalkReplyMessage) Serializer() message.Serializer {
	return walkReplyMessageSerializer
}

func (WalkReplyMessage) Deserializer() message.Deserializer {
	return walkReplyMessageSerializer
}

func (WalkReplyMessageSerializer) Serialize(msg message.Message) []byte {
	walkReply := msg.(WalkReplyMessage)
	return SerializePeerWithIDChainArray(walkReply.Sample)
}

func (WalkReplyMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	_, sample := DeserializePeerWithIDChainArray(msgBytes)
	return WalkReplyMessage{Sample: sample}
}

const broadcastMessageID = 1012

func NewBroadcastMessage(wrappedMsg body_types.Message) BroadcastMessage {
	return BroadcastMessage{
		Message: wrappedMsg,
	}
}

type BroadcastMessage struct {
	Message body_types.Message
}

type BroadcastMessageSerializer struct{}

func (BroadcastMessage) Type() message.ID {
	return broadcastMessageID
}

var broadcastMessageSerializer = BroadcastMessageSerializer{}

func (BroadcastMessage) Serializer() message.Serializer {
	return broadcastMessageSerializer
}

func (BroadcastMessage) Deserializer() message.Deserializer {
	return broadcastMessageSerializer
}

func (BroadcastMessageSerializer) Serialize(msg message.Message) []byte {
	broadcast := msg.(BroadcastMessage)
	msgBytes, err := json.Marshal(broadcast.Message)

	if err != nil {
		panic(err)
	}

	return msgBytes
}

func (BroadcastMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	broadcastMsg := body_types.Message{}
	err := json.Unmarshal(msgBytes, &broadcastMsg)

	if err != nil {
		panic(err)
	}
	return BroadcastMessage{Message: broadcastMsg}
}

/*

const switchMessageID = 1012

func NewSwitchMessage(parent,
	grandparent *PeerWithIDChain,
	newChildren []*PeerWithIDChain,
	connectAsChild,
	connectAsParent bool) SwitchMessage {
	return SwitchMessage{
		Parent:          parent,
		ConnectAsChild:  connectAsChild,
		ConnectAsParent: connectAsParent,
	}
}

type SwitchMessage struct {
	GrandParent *PeerWithIDChain
	Children    []*PeerWithIDChain

	Parent          *PeerWithIDChain
	ConnectAsChild  bool
	ConnectAsParent bool
}

type SwitchMessageSerializer struct{}

func (SwitchMessage) Type() message.ID {
	return switchMessageID
}

var switchMessageSerializer = SwitchMessageSerializer{}

func (SwitchMessage) Serializer() message.Serializer {
	return switchMessageSerializer
}

func (SwitchMessage) Deserializer() message.Deserializer {
	return switchMessageSerializer
}

func (SwitchMessageSerializer) Serialize(msg message.Message) []byte {
	switchMsg := msg.(SwitchMessage)
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
	var gparent *PeerWithIDChain
	if msgBytes[bufPos] == 1 {
		n, gparent = UnmarshalPeerWithIdChain(msgBytes[bufPos:])
		bufPos += n
	} else {
		bufPos++
	}
	n, children := DeserializePeerWithIDChainArray(msgBytes[bufPos:])
	bufPos += n
	connectAsChild := msgBytes[bufPos] == 1
	bufPos++
	connectAsParent := msgBytes[bufPos] == 1
	return NewSwitchMessage(parent, gparent, children, connectAsChild, connectAsParent)
}

*/
