package protocol

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/message"
	"k8s.io/apimachinery/pkg/util/json"
)

// ADD INTEREST SET

var installNeighInterestSetMsgSerializerVar = installNeighInterestSetMsgSerializer{}

const InstallNeighInterestSetMsgID = 6000

type InstallNeighInterestSetMsg struct {
	InterestSets map[uint64]neighInterestSet
}

func NewInstallNeighInterestSetMessage(interestSets map[uint64]neighInterestSet) InstallNeighInterestSetMsg {
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

var removeNeighInterestSetMsgSerializerVar = removeNeighInterestSetMsgSerializer{}

// REMOVE INTEREST SET

const removeNeighInterestSetMsgID = 6001

type RemoveNeighInterestSetMsg struct {
	InterestSetID uint64
}

func NewRemoveNeighInterestSetMessage(id uint64) RemoveNeighInterestSetMsg {
	return RemoveNeighInterestSetMsg{
		InterestSetID: id,
	}
}

func (RemoveNeighInterestSetMsg) Type() message.ID {
	return removeNeighInterestSetMsgID
}

func (RemoveNeighInterestSetMsg) Serializer() message.Serializer {
	return removeNeighInterestSetMsgSerializerVar
}

func (RemoveNeighInterestSetMsg) Deserializer() message.Deserializer {
	return removeNeighInterestSetMsgSerializerVar
}

type removeNeighInterestSetMsgSerializer struct{}

func (removeNeighInterestSetMsgSerializer) Serialize(m message.Message) []byte {
	mConverted := m.(RemoveNeighInterestSetMsg)
	msgBytes, err := json.Marshal(mConverted)

	if err != nil {
		panic(err)
	}

	return msgBytes
}

func (removeNeighInterestSetMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	toDeserialize := RemoveNeighInterestSetMsg{}

	err := json.Unmarshal(msgBytes, &toDeserialize)

	if err != nil {
		panic(err)
	}

	return toDeserialize
}

var propagateInterestSetMetricsMsgSerializerVar = propagateInterestSetMetricsMsgSerializer{}

const propagateInterestSetMetricsMsgID = 6002

type PropagateInterestSetMetricsMsg struct {
	InterestSetID uint64
	Metrics       []body_types.TimeseriesDTO
	TTL           int
}

func NewPropagateInterestSetMetricsMessage(
	interestSetID uint64,
	tsArr []body_types.TimeseriesDTO,
	ttl int,
) PropagateInterestSetMetricsMsg {

	toReturn := PropagateInterestSetMetricsMsg{
		InterestSetID: interestSetID,
		TTL:           ttl,
		Metrics:       tsArr,
	}

	return toReturn
}

func (PropagateInterestSetMetricsMsg) Type() message.ID {
	return propagateInterestSetMetricsMsgID
}

func (PropagateInterestSetMetricsMsg) Serializer() message.Serializer {
	return propagateInterestSetMetricsMsgSerializerVar
}

func (PropagateInterestSetMetricsMsg) Deserializer() message.Deserializer {
	return propagateInterestSetMetricsMsgSerializerVar
}

type propagateInterestSetMetricsMsgSerializer struct{}

func (propagateInterestSetMetricsMsgSerializer) Serialize(m message.Message) []byte {
	mConverted := m.(PropagateInterestSetMetricsMsg)

	msgBytes, err := json.Marshal(mConverted)

	if err != nil {
		panic(err)
	}

	return msgBytes
}

func (propagateInterestSetMetricsMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	toDeserialize := PropagateInterestSetMetricsMsg{}
	err := json.Unmarshal(msgBytes, &toDeserialize)

	if err != nil {
		panic(err)
	}

	return toDeserialize
}
