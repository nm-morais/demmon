package protocol

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
)

type InView struct {
	Grandparent     *PeerWithIDChain
	Parent          *PeerWithIDChain
	Children        []*PeerWithIDChain
	Siblings        []*PeerWithIDChain
	IsSelfBootstrap bool
}

type GetNeighboursReq struct {
	Key string
}

const GetNeighboursReqID = 2000

func (r GetNeighboursReq) ID() protocol.ID {
	return GetNeighboursReqID
}

func NewGetNeighboursReq(key string) request.Request {
	return GetNeighboursReq{
		Key: key,
	}
}

const GetNeighboursReqReplyID = 2001

type GetNeighboutsReply struct {
	InView InView
	Key    string
}

func (r GetNeighboutsReply) ID() protocol.ID {
	return GetNeighboursReqReplyID
}

func NewGetNeighboursReqReply(key string, view InView) request.Reply {
	return GetNeighboutsReply{
		InView: view,
		Key:    key,
	}
}

type GetIDReq struct {
}

const GetIDReqID = 2002

func (r GetIDReq) ID() protocol.ID {
	return GetIDReqID
}

func NewGetIDReq() request.Request {
	return GetIDReq{}
}

const GetIDReqReplyID = 2003

type GetIDReply struct {
	CurrID PeerIDChain
}

func (r GetIDReply) ID() protocol.ID {
	return GetIDReqReplyID
}

func NewGetIDReqReply(currID PeerIDChain) request.Reply {
	return GetIDReply{
		CurrID: currID,
	}
}

const BroadcastMessageReqID = 2004

type BroadcastMessageRequest struct {
	Message body_types.Message
}

func (r BroadcastMessageRequest) ID() protocol.ID {
	return BroadcastMessageReqID
}

func NewBroadcastMessageRequest(msg body_types.Message) request.Reply {
	return BroadcastMessageRequest{
		Message: msg,
	}
}
