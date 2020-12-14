package protocol

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
)

type AddNeighborhoodInterestSetReq struct {
	InterestSetID uint64
	InterestSet   body_types.NeighborhoodInterestSet
}

const AddNeighborhoodInterestSetReqID = 2000

func (r AddNeighborhoodInterestSetReq) ID() protocol.ID {
	return AddNeighborhoodInterestSetReqID
}

func NewAddNeighborhoodInterestSetReq(key uint64, interestSet body_types.NeighborhoodInterestSet) request.Request {
	return AddNeighborhoodInterestSetReq{
		InterestSetID: key,
		InterestSet:   interestSet,
	}
}

type RemoveNeighborhoodInterestSetReq struct {
	InterestSetID uint64
}

const RemoveNeighborhoodInterestSetReqID = 2000

func (r RemoveNeighborhoodInterestSetReq) ID() protocol.ID {
	return RemoveNeighborhoodInterestSetReqID
}

func NewRemoveNeighborhoodInterestSetReq(key uint64) request.Request {
	return RemoveNeighborhoodInterestSetReq{
		InterestSetID: key,
	}
}
