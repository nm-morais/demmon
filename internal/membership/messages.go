package membership

import (
	"encoding/binary"
	"time"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
)

// -------------- Join --------------

const JoinMessageID = 1000

type JoinMessage struct {
}

func (JoinMessage) Type() message.ID {
	return JoinMessageID
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

const JoinReplyMessageID = 1001

type JoinReplyMessage struct {
	Children      []peer.Peer
	Level         uint16
	ParentLatency time.Duration
}

func (JoinReplyMessage) Type() message.ID {
	return JoinReplyMessageID
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
	msgBytes := make([]byte, 10)
	binary.BigEndian.PutUint16(msgBytes[0:2], jrMsg.Level)
	binary.BigEndian.PutUint64(msgBytes[2:10], uint64(jrMsg.ParentLatency))
	return append(msgBytes, peer.SerializePeerArray(jrMsg.Children)...)
}

func (JoinReplyMsgSerializer) Deserialize(msgBytes []byte) message.Message {

	level := binary.BigEndian.Uint16(msgBytes[0:2])
	parentLatency := time.Duration(binary.BigEndian.Uint64(msgBytes[2:10]))
	_, hosts := peer.DeserializePeerArray(msgBytes[10:])
	return JoinReplyMessage{Children: hosts, Level: level, ParentLatency: parentLatency}
}

// -------------- Update parent --------------

const UpdateParentMessageID = 1002

type UpdateParentMessage struct {
	GrandParent peer.Peer
	ParentLevel uint16
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
	bufPos := 0
	msgBytes := make([]byte, 4)
	binary.BigEndian.PutUint16(msgBytes[bufPos:bufPos+2], uPMsg.ParentLevel)
	bufPos += 2
	if uPMsg.GrandParent != nil {
		msgBytes[bufPos] = 1
		gParentBytes := uPMsg.GrandParent.SerializeToBinary()
		msgBytes = append(msgBytes, gParentBytes...)
	} else {
		msgBytes[bufPos] = 0
	}
	return msgBytes
}

func (UpdateParentMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
	bufPos += 2
	var gParentFinal peer.Peer
	if msgBytes[bufPos] == 1 {
		bufPos++
		_, parent := peer.DeserializePeer(msgBytes[bufPos:])
		gParentFinal = parent
	}
	return UpdateParentMessage{GrandParent: gParentFinal, ParentLevel: level}
}

// -------------- JoinAsParent --------------

const JoinAsParentMessageID = 1003

type JoinAsParentMessage struct {
	Children []peer.Peer
	Level    uint16
}

func (JoinAsParentMessage) Type() message.ID {
	return JoinAsParentMessageID
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
	toSend := make([]byte, 4)
	bufPos := 0
	binary.BigEndian.PutUint16(toSend[bufPos:bufPos+2], japMsg.Level)
	toSend = append(toSend, peer.SerializePeerArray(japMsg.Children)...)
	return toSend
}

func (JoinAsParentMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
	bufPos += 2
	_, children := peer.DeserializePeerArray(msgBytes[bufPos:])
	return JoinAsParentMessage{
		Level:    level,
		Children: children,
	}
}

// -------------- Join As Child --------------

const JoinAsChildMessageID = 1004

type JoinAsChildMessage struct {
}

func (JoinAsChildMessage) Type() message.ID {
	return JoinAsChildMessageID
}

func (JoinAsChildMessage) Serializer() message.Serializer {
	return joinAsChildMsgSerializer
}

func (JoinAsChildMessage) Deserializer() message.Deserializer {
	return joinAsChildMsgSerializer
}

type JoinAsChildMsgSerializer struct{}

var joinAsChildMsgSerializer = JoinAsChildMsgSerializer{}

func (JoinAsChildMsgSerializer) Serialize(msg message.Message) []byte { return []byte{} }

func (JoinAsChildMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	return JoinAsChildMessage{}
}
