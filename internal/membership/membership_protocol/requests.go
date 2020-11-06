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
