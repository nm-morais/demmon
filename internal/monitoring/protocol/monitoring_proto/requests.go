package monitoring_proto

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
)

type AddNeighborhoodInterestSetReq struct {
	InterestSetId uint64
	InterestSet   body_types.NeighborhoodInterestSet
}

const AddNeighborhoodInterestSetReqId = 2000

func (r AddNeighborhoodInterestSetReq) ID() protocol.ID {
	return AddNeighborhoodInterestSetReqId
}

func NewAddNeighborhoodInterestSetReq(key uint64, interestSet body_types.NeighborhoodInterestSet) request.Request {
	return AddNeighborhoodInterestSetReq{
		InterestSetId: key,
		InterestSet:   interestSet,
	}
}

type RemoveNeighborhoodInterestSetReq struct {
	InterestSetId uint64
}

const RemoveNeighborhoodInterestSetReqId = 2000

func (r RemoveNeighborhoodInterestSetReq) ID() protocol.ID {
	return RemoveNeighborhoodInterestSetReqId
}

func NewRemoveNeighborhoodInterestSetReq(key uint64) request.Request {
	return RemoveNeighborhoodInterestSetReq{
		InterestSetId: key,
	}
}
