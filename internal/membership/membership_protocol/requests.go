package membership_protocol

import (
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
)

type InView struct {
	Grandparent *PeerWithIdChain
	Parent      *PeerWithIdChain
	Children    []*PeerWithIdChain
	Siblings    []*PeerWithIdChain
}

type GetNeighboursReq struct {
	Key string
}

const GetNeighboursReqId = 2000

func (r GetNeighboursReq) ID() protocol.ID {
	return GetNeighboursReqId
}

func NewGetNeighboursReq(key string) request.Request {
	return GetNeighboursReq{
		Key: key,
	}
}

const GetNeighboursReqReplyId = 2001

type GetNeighboutsReply struct {
	InView InView
	Key    string
}

func (r GetNeighboutsReply) ID() protocol.ID {
	return GetNeighboursReqReplyId
}

func NewGetNeighboursReqReply(key string, view InView) request.Reply {
	return GetNeighboutsReply{
		InView: view,
		Key:    key,
	}
}

type GetIDReq struct {
}

const GetIDReqId = 2002

func (r GetIDReq) ID() protocol.ID {
	return GetIDReqId
}

func NewGetIDReq() request.Request {
	return GetIDReq{}
}

const GetIDReqReplyId = 2003

type GetIDReply struct {
	CurrID PeerIDChain
}

func (r GetIDReply) ID() protocol.ID {
	return GetIDReqReplyId
}

func NewGetIDReqReply(currId PeerIDChain) request.Reply {
	return GetIDReply{
		CurrID: currId,
	}
}
