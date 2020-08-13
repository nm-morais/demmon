package membership

import (
	"encoding/binary"
	"github.com/nm-morais/go-babel/pkg/message"
	"time"
)

const JoinMessageID = 1000

type JoinMessage struct {
	Timestamp time.Time
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

func (JoinMsgSerializer) Serialize(msg message.Message) []byte {
	joinMsg := msg.(JoinMessage)
	toReturn := make([]byte, 64)
	binary.BigEndian.PutUint64(toReturn, uint64(joinMsg.Timestamp.UnixNano()))
	return toReturn
}

func (JoinMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	return JoinMessage{Timestamp: time.Unix(0, int64(binary.BigEndian.Uint64(msgBytes)))}
}

const JoinReplyMessageID = 1001

type JoinReplyMessage struct {
	Timestamp time.Time
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
	joinReplyMsg := msg.(JoinReplyMessage)
	toReturn := make([]byte, 64)

	return toReturn
}

func (JoinReplyMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	return JoinReplyMessage{Timestamp: time.Unix(0, int64(binary.BigEndian.Uint64(msgBytes)))}
}

const JoinLevelMessageID = 1002
