package membership

import (
	"encoding/binary"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"net"
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
	ParentLatency uint64
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
	msgBytes := make([]byte, 12)
	binary.BigEndian.PutUint16(msgBytes[0:2], jrMsg.Level)
	binary.BigEndian.PutUint64(msgBytes[2:10], jrMsg.ParentLatency)
	binary.BigEndian.PutUint16(msgBytes[10:12], uint16(len(jrMsg.Children)))
	for _, addr := range jrMsg.Children {
		hostSizeBytes := make([]byte, 4)
		hostBytes := []byte(addr.Addr().String())
		binary.BigEndian.PutUint32(hostSizeBytes[0:4], uint32(len(hostBytes)))
		msgBytes = append(msgBytes, hostSizeBytes...)
		msgBytes = append(msgBytes, hostBytes...)
	}
	return msgBytes
}

func (JoinReplyMsgSerializer) Deserialize(msgBytes []byte) message.Message {

	level := binary.BigEndian.Uint16(msgBytes[0:2])
	parentLatency := binary.BigEndian.Uint64(msgBytes[2:10])
	nrHosts := binary.BigEndian.Uint16(msgBytes[10:12])
	hosts := make([]peer.Peer, nrHosts)
	bufPos := 12
	for i := 0; uint16(i) < nrHosts; i++ {
		addrSize := int(binary.BigEndian.Uint32(msgBytes[bufPos : bufPos+4]))
		bufPos += 4
		addr := string(msgBytes[bufPos : bufPos+addrSize])
		bufPos += addrSize
		resolved, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			panic(err)
		}
		hosts[i] = peer.NewPeer(resolved)
	}
	return JoinReplyMessage{hosts, level, parentLatency}
}

// -------------- Update parent --------------

const UpdateParentMessageID = 1002

type UpdateParentMessage struct {
	Parent      peer.Peer
	GrandParent peer.Peer
	Level       uint16
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
	msgBytes := make([]byte, 4)
	binary.BigEndian.PutUint16(msgBytes[0:2], uPMsg.Level)
	binary.BigEndian.PutUint16(msgBytes[2:4], uint16(len(uPMsg.GrandParent.ToString())))
	msgBytes = append([]byte(uPMsg.GrandParent.ToString()))
	aux := make([]byte, 2)
	binary.BigEndian.PutUint16(aux[0:2], uint16(len(uPMsg.Parent.ToString())))
	msgBytes = append(msgBytes, aux...)
	msgBytes = append(msgBytes, []byte(uPMsg.Parent.ToString())...)

	return msgBytes
}

func (UpdateParentMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
	bufPos += 2
	gparentSize := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
	bufPos += 2
	gparentStr := string(msgBytes[bufPos:gparentSize])
	bufPos += int(gparentSize)
	gParent, err := net.ResolveTCPAddr("tcp", gparentStr)
	if err != nil {
		panic(err)
	}
	parentSize := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
	bufPos += 2
	parentStr := string(msgBytes[bufPos : bufPos+int(parentSize)])
	parent, err := net.ResolveTCPAddr("tcp", parentStr)
	if err != nil {
		panic(err)
	}

	return UpdateParentMessage{peer.NewPeer(parent), peer.NewPeer(gParent), level}
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
	bufPos += 2
	binary.BigEndian.PutUint16(toSend[bufPos:bufPos+2], japMsg.Level)
	for _, child := range japMsg.Children {
		childBytes := []byte(child.ToString())
		binary.BigEndian.PutUint16(toSend[bufPos:bufPos+2], uint16(len(childBytes)))
		bufPos += 2
		toSend = append(toSend, childBytes...)
	}
	return []byte{}
}

func (JoinAsParentMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	level := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
	bufPos += 2
	nrChildren := int(binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2]))
	bufPos += 2
	children := make([]peer.Peer, nrChildren)
	for i := 0; i < nrChildren; i++ {
		childSize := binary.BigEndian.Uint16(msgBytes[bufPos : bufPos+2])
		bufPos += 2
		childBytes := msgBytes[bufPos : bufPos+int(childSize)]
		child, err := net.ResolveTCPAddr("tcp", string(childBytes))
		if err != nil {
			panic(err)
		}
		children[i] = peer.NewPeer(child)
		bufPos += int(childSize)
	}

	return JoinAsParentMessage{
		Level:    level,
		Children: children,
	}
}

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
