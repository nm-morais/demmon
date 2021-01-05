package protocol

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/message"
	"k8s.io/apimachinery/pkg/util/json"
)

// NEIGH INTEREST SETS

var installNeighInterestSetMsgSerializerVar = installNeighInterestSetMsgSerializer{}

const InstallNeighInterestSetMsgID = 6000

type InstallNeighInterestSetMsg struct {
	InterestSets map[int64]neighInterestSet
}

func NewInstallNeighInterestSetMessage(interestSets map[int64]neighInterestSet) InstallNeighInterestSetMsg {
	return InstallNeighInterestSetMsg{
		InterestSets: interestSets,
	}
}

func (InstallNeighInterestSetMsg) Type() message.ID {
	return InstallNeighInterestSetMsgID
}

func (InstallNeighInterestSetMsg) Serializer() message.Serializer {
	return installNeighInterestSetMsgSerializerVar
}

func (InstallNeighInterestSetMsg) Deserializer() message.Deserializer {
	return installNeighInterestSetMsgSerializerVar
}

type installNeighInterestSetMsgSerializer struct{}

func (installNeighInterestSetMsgSerializer) Serialize(m message.Message) []byte {
	mConverted := m.(InstallNeighInterestSetMsg)
	msgBytes, err := json.Marshal(mConverted)

	if err != nil {
		panic(err)
	}

	return msgBytes
}

func (installNeighInterestSetMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	toDeserialize := InstallNeighInterestSetMsg{}
	err := json.Unmarshal(msgBytes, &toDeserialize)

	if err != nil {
		panic(err)
	}

	return toDeserialize
}

// var removeNeighInterestSetMsgSerializerVar = removeNeighInterestSetMsgSerializer{}

// REMOVE INTEREST SET

// const removeNeighInterestSetMsgID = 6001

// type RemoveNeighInterestSetMsg struct {
// 	InterestSetID uint64
// }

// func NewRemoveNeighInterestSetMessage(id uint64) RemoveNeighInterestSetMsg {
// 	return RemoveNeighInterestSetMsg{
// 		InterestSetID: id,
// 	}
// }

// func (RemoveNeighInterestSetMsg) Type() message.ID {
// 	return removeNeighInterestSetMsgID
// }

// func (RemoveNeighInterestSetMsg) Serializer() message.Serializer {
// 	return removeNeighInterestSetMsgSerializerVar
// }

// func (RemoveNeighInterestSetMsg) Deserializer() message.Deserializer {
// 	return removeNeighInterestSetMsgSerializerVar
// }

// type removeNeighInterestSetMsgSerializer struct{}

// func (removeNeighInterestSetMsgSerializer) Serialize(m message.Message) []byte {
// 	mConverted := m.(RemoveNeighInterestSetMsg)
// 	msgBytes, err := json.Marshal(mConverted)

// 	if err != nil {
// 		panic(err)
// 	}

// 	return msgBytes
// }

// func (removeNeighInterestSetMsgSerializer) Deserialize(msgBytes []byte) message.Message {
// 	toDeserialize := RemoveNeighInterestSetMsg{}

// 	err := json.Unmarshal(msgBytes, &toDeserialize)

// 	if err != nil {
// 		panic(err)
// 	}

// 	return toDeserialize
// }

var propagateNeighInterestSetMetricsMsgSerializerVar = propagateNeighInterestSetMetricsMsgSerializer{}

const propagateInterestSetMetricsMsgID = 6002

type PropagateNeighInterestSetMetricsMsg struct {
	InterestSetID int64
	Metrics       []body_types.TimeseriesDTO
	TTL           int
}

func NewPropagateNeighInterestSetMetricsMessage(
	interestSetID int64,
	tsArr []body_types.TimeseriesDTO,
	ttl int,
) PropagateNeighInterestSetMetricsMsg {

	toReturn := PropagateNeighInterestSetMetricsMsg{
		InterestSetID: interestSetID,
		TTL:           ttl,
		Metrics:       tsArr,
	}

	return toReturn
}

func (PropagateNeighInterestSetMetricsMsg) Type() message.ID {
	return propagateInterestSetMetricsMsgID
}

func (PropagateNeighInterestSetMetricsMsg) Serializer() message.Serializer {
	return propagateNeighInterestSetMetricsMsgSerializerVar
}

func (PropagateNeighInterestSetMetricsMsg) Deserializer() message.Deserializer {
	return propagateNeighInterestSetMetricsMsgSerializerVar
}

type propagateNeighInterestSetMetricsMsgSerializer struct{}

func (propagateNeighInterestSetMetricsMsgSerializer) Serialize(m message.Message) []byte {
	mConverted := m.(PropagateNeighInterestSetMetricsMsg)
	msgBytes, err := json.Marshal(mConverted)

	if err != nil {
		panic(err)
	}

	return msgBytes
}

func (propagateNeighInterestSetMetricsMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	toDeserialize := PropagateNeighInterestSetMetricsMsg{}
	err := json.Unmarshal(msgBytes, &toDeserialize)

	if err != nil {
		panic(err)
	}

	return toDeserialize
}

// TREE AGG FUNCS

const propagateTreeAggFuncMetricsMsgID = 6003

var propagateTreeAggFuncMsgSerializerVar = propagateTreeAggFuncMsgSerializer{}

type PropagateTreeAggFuncMetricsMsg struct {
	InterestSetID int64
	Value         *body_types.ObservableDTO
}

func NewPropagateTreeAggFuncMetricsMessage(
	interestSetID int64,
	value *body_types.ObservableDTO,
) PropagateTreeAggFuncMetricsMsg {

	toReturn := PropagateTreeAggFuncMetricsMsg{
		InterestSetID: interestSetID,
		Value:         value,
	}

	return toReturn
}

func (PropagateTreeAggFuncMetricsMsg) Type() message.ID {
	return propagateInterestSetMetricsMsgID
}

func (PropagateTreeAggFuncMetricsMsg) Serializer() message.Serializer {
	return propagateTreeAggFuncMsgSerializerVar
}

func (PropagateTreeAggFuncMetricsMsg) Deserializer() message.Deserializer {
	return propagateTreeAggFuncMsgSerializerVar
}

type propagateTreeAggFuncMsgSerializer struct{}

func (propagateTreeAggFuncMsgSerializer) Serialize(m message.Message) []byte {
	mConverted := m.(PropagateTreeAggFuncMetricsMsg)
	msgBytes, err := json.Marshal(mConverted)

	if err != nil {
		panic(err)
	}

	return msgBytes
}

func (propagateTreeAggFuncMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	toDeserialize := PropagateTreeAggFuncMetricsMsg{}
	err := json.Unmarshal(msgBytes, &toDeserialize)

	if err != nil {
		panic(err)
	}

	return toDeserialize
}

var installTreeAggFuncMsgSerializerVar = installTreeAggFuncMsgSerializer{}

const InstallTreeAggFuncMsgID = 6004

type InstallTreeAggFuncMsg struct {
	InterestSets map[int64]treeAggSet
}

func NewInstallTreeAggFuncMessage(interestSets map[int64]treeAggSet) InstallTreeAggFuncMsg {
	return InstallTreeAggFuncMsg{
		InterestSets: interestSets,
	}
}

func (InstallTreeAggFuncMsg) Type() message.ID {
	return InstallTreeAggFuncMsgID
}

func (InstallTreeAggFuncMsg) Serializer() message.Serializer {
	return installTreeAggFuncMsgSerializerVar
}

func (InstallTreeAggFuncMsg) Deserializer() message.Deserializer {
	return installTreeAggFuncMsgSerializerVar
}

type installTreeAggFuncMsgSerializer struct{}

func (installTreeAggFuncMsgSerializer) Serialize(m message.Message) []byte {
	mConverted := m.(InstallTreeAggFuncMsg)
	msgBytes, err := json.Marshal(mConverted)

	if err != nil {
		panic(err)
	}

	return msgBytes
}

func (installTreeAggFuncMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	toDeserialize := InstallTreeAggFuncMsg{}
	err := json.Unmarshal(msgBytes, &toDeserialize)

	if err != nil {
		panic(err)
	}

	return toDeserialize
}
