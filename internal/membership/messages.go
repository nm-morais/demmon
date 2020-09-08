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
	Children      []peer.Peer
	Level         uint16
	ParentLatency time.Duration
}

func NewJoinReplyMessage(children []peer.Peer, level uint16, parentLatency time.Duration) joinReplyMessage {
	return joinReplyMessage{
		Children:      children,
		Level:         level,
		ParentLatency: parentLatency,
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
	binary.BigEndian.PutUint16(msgBytes[0:2], jrMsg.Level)
	binary.BigEndian.PutUint64(msgBytes[2:10], uint64(jrMsg.ParentLatency))
	return append(msgBytes, peer.SerializePeerArray(jrMsg.Children)...)
}

func (JoinReplyMsgSerializer) Deserialize(msgBytes []byte) message.Message {

	level := binary.BigEndian.Uint16(msgBytes[0:2])
	parentLatency := time.Duration(binary.BigEndian.Uint64(msgBytes[2:10]))
	_, hosts := peer.DeserializePeerArray(msgBytes[10:])
	return joinReplyMessage{Children: hosts, Level: level, ParentLatency: parentLatency}
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

// -------------- JoinAsParent --------------
const joinAsParentMessageID = 1003

type joinAsParentMessage struct {
	ExpectedParent  peer.Peer
	ProposedId      PeerIDChain
	Level           uint16
	MeasuredLatency time.Duration
}

func NewJoinAsParentMessage(expectedParent peer.Peer, proposedId PeerIDChain, level uint16, measuredLatency time.Duration) joinAsParentMessage {
	return joinAsParentMessage{
		ExpectedParent:  expectedParent,
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
	toSend = append(toSend, japMsg.ExpectedParent.SerializeToBinary()...)
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
	parentSize, parent := peer.DeserializePeer(msgBytes[bufPos:])
	bufPos += parentSize
	_, proposedId := DeserializePeerIDChain(msgBytes[bufPos:])
	return joinAsParentMessage{
		MeasuredLatency: measuredLatency,
		Level:           level,
		ExpectedParent:  parent,
		ProposedId:      proposedId,
	}
}

// -------------- Join As Child --------------

const joinAsChildMessageID = 1005

type joinAsChildMessage struct {
	Children []peer.Peer
}

func NewJoinAsChildMessage(children []peer.Peer) joinAsChildMessage {
	return joinAsChildMessage{
		Children: children,
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
	return peer.SerializePeerArray(msg.(joinAsChildMessage).Children)
}

func (JoinAsChildMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	_, children := peer.DeserializePeerArray(msgBytes)
	return joinAsChildMessage{Children: children}
}

const joinAsChildMessageReplyID = 1006

type joinAsChildMessageReply struct {
	ProposedId PeerIDChain
	Accepted   bool
}

func NewJoinAsChildMessageReply(accepted bool, proposedId PeerIDChain) joinAsChildMessageReply {
	return joinAsChildMessageReply{
		ProposedId: proposedId,
		Accepted:   accepted,
	}
}

func (joinAsChildMessageReply) Type() message.ID {
	return joinAsChildMessageReplyID
}

func (joinAsChildMessageReply) Serializer() message.Serializer {
	return joinAsChildMsgReplySerializer
}

func (joinAsChildMessageReply) Deserializer() message.Deserializer {
	return joinAsChildMsgReplySerializer
}

type JoinAsChildMsgReplySerializer struct{}

var joinAsChildMsgReplySerializer = JoinAsChildMsgReplySerializer{}

func (JoinAsChildMsgReplySerializer) Serialize(msg message.Message) []byte {
	jacMsgR := msg.(joinAsChildMessageReply)
	if jacMsgR.Accepted {
		return []byte{1}
	}
	msgBytes := []byte{0}
	msgBytes = append(msgBytes, SerializePeerIDChain(jacMsgR.ProposedId)...)
	return msgBytes
}

func (JoinAsChildMsgReplySerializer) Deserialize(msgBytes []byte) message.Message {
	accepted := msgBytes[0] == 1
	if !accepted {
		return joinAsChildMessageReply{Accepted: false}
	}
	_, proposedId := DeserializePeerIDChain(msgBytes[1:])
	return joinAsChildMessageReply{
		ProposedId: proposedId,
		Accepted:   accepted,
	}

}
