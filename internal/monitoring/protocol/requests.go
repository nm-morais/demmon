package protocol

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
)

const AddNeighborhoodInterestSetReqID = 2000

type AddNeighborhoodInterestSetReq struct {
	Id          int64
	InterestSet body_types.NeighborhoodInterestSet
}

func (r AddNeighborhoodInterestSetReq) ID() protocol.ID {
	return AddNeighborhoodInterestSetReqID
}

func NewAddNeighborhoodInterestSetReq(id int64, interestSet body_types.NeighborhoodInterestSet) request.Request {
	return AddNeighborhoodInterestSetReq{
		Id:          id,
		InterestSet: interestSet,
	}
}

type RemoveNeighborhoodInterestSetReq struct {
	InterestSetID int64
}

const RemoveNeighborhoodInterestSetReqID = 2001

func (r RemoveNeighborhoodInterestSetReq) ID() protocol.ID {
	return RemoveNeighborhoodInterestSetReqID
}

func NewRemoveNeighborhoodInterestSetReq(key int64) request.Request {
	return RemoveNeighborhoodInterestSetReq{
		InterestSetID: key,
	}
}

// TreeAggregationFunction

const AddTreeAggregationFuncReqID = 2002

type AddTreeAggregationFuncReq struct {
	Id          int64
	InterestSet *body_types.TreeAggregationSet
}

func (r AddTreeAggregationFuncReq) ID() protocol.ID {
	return AddTreeAggregationFuncReqID
}

func NewAddTreeAggregationFuncReq(id int64, interestSet *body_types.TreeAggregationSet) request.Request {
	return AddTreeAggregationFuncReq{
		Id:          id,
		InterestSet: interestSet,
	}
}

type RemoveTreeAggregationFuncReq struct {
	InterestSetID uint64
}

const RemoveTreeAggregationFuncReqID = 2003

func (r RemoveTreeAggregationFuncReq) ID() protocol.ID {
	return RemoveTreeAggregationFuncReqID
}

func NewRemoveTreeAggregationFuncReq(key uint64) request.Request {
	return RemoveTreeAggregationFuncReq{
		InterestSetID: key,
	}
}

// GlobalAggregationFunction

const AddGlobalAggregationFuncReqID = 2005

type AddGlobalAggregationFuncReq struct {
	Id      int64
	AggFunc body_types.GlobalAggregationFunction
}

func (r AddGlobalAggregationFuncReq) ID() protocol.ID {
	return AddGlobalAggregationFuncReqID
}

func NewAddGlobalAggregationFuncReq(id int64, interestSet body_types.GlobalAggregationFunction) request.Request {
	return AddGlobalAggregationFuncReq{
		Id:      id,
		AggFunc: interestSet,
	}
}

type RemoveGlobalAggregationFuncReq struct {
	InterestSetID uint64
}

const RemoveGlobalAggregationFuncReqID = 2006

func (r RemoveGlobalAggregationFuncReq) ID() protocol.ID {
	return RemoveGlobalAggregationFuncReqID
}

func NewRemoveGlobalAggregationFuncReq(key uint64) request.Request {
	return RemoveGlobalAggregationFuncReq{
		InterestSetID: key,
	}
}
