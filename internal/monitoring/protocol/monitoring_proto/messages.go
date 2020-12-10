package monitoring_proto

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/demmon/internal/monitoring/tsdb"
	"github.com/nm-morais/go-babel/pkg/message"
	"k8s.io/apimachinery/pkg/util/json"
)

// ADD INTEREST SET

var installNeighInterestSetMsgSerializerVar = installNeighInterestSetMsgSerializer{}

const InstallNeighInterestSetMsgID = 6000

type installNeighInterestSetMsg struct {
	InterestSets map[uint64]body_types.NeighborhoodInterestSet
}

func NewInstallNeighInterestSetMessage(interestSets map[uint64]body_types.NeighborhoodInterestSet) installNeighInterestSetMsg {
	return installNeighInterestSetMsg{
		InterestSets: interestSets,
	}
}

func (installNeighInterestSetMsg) Type() message.ID {
	return InstallNeighInterestSetMsgID
}

func (installNeighInterestSetMsg) Serializer() message.Serializer {
	return installNeighInterestSetMsgSerializerVar
}

func (installNeighInterestSetMsg) Deserializer() message.Deserializer {
	return installNeighInterestSetMsgSerializerVar
}

type installNeighInterestSetMsgSerializer struct{}

func (installNeighInterestSetMsgSerializer) Serialize(m message.Message) []byte {
	mConverted := m.(installNeighInterestSetMsg)
	msgBytes, err := json.Marshal(mConverted)
	if err != nil {
		panic(err)
	}
	return msgBytes
}

func (installNeighInterestSetMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	toDeserialize := installNeighInterestSetMsg{}
	err := json.Unmarshal(msgBytes, &toDeserialize)
	if err != nil {
		panic(err)
	}
	return toDeserialize
}

var removeNeighInterestSetMsgSerializerVar = removeNeighInterestSetMsgSerializer{}

// REMOVE INTEREST SET

const removeNeighInterestSetMsgID = 6001

type removeNeighInterestSetMsg struct {
	InterestSetId uint64
}

func NewRemoveNeighInterestSetMessage(id uint64) removeNeighInterestSetMsg {
	return removeNeighInterestSetMsg{
		InterestSetId: id,
	}
}

func (removeNeighInterestSetMsg) Type() message.ID {
	return removeNeighInterestSetMsgID
}

func (removeNeighInterestSetMsg) Serializer() message.Serializer {
	return removeNeighInterestSetMsgSerializerVar
}

func (removeNeighInterestSetMsg) Deserializer() message.Deserializer {
	return removeNeighInterestSetMsgSerializerVar
}

type removeNeighInterestSetMsgSerializer struct{}

func (removeNeighInterestSetMsgSerializer) Serialize(m message.Message) []byte {
	mConverted := m.(removeNeighInterestSetMsg)
	msgBytes, err := json.Marshal(mConverted)
	if err != nil {
		panic(err)
	}
	return msgBytes
}

func (removeNeighInterestSetMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	toDeserialize := removeNeighInterestSetMsg{}
	err := json.Unmarshal(msgBytes, &toDeserialize)
	if err != nil {
		panic(err)
	}
	return toDeserialize
}

var propagateInterestSetMetricsMsgSerializerVar = propagateInterestSetMetricsMsgSerializer{}

const propagateInterestSetMetricsMsgID = 6002

type propagateInterestSetMetricsMsg struct {
	InterestSetId uint64
	Metrics       []body_types.Timeseries
}

func NewPropagateInterestSetMetricsMessage(interestSetId uint64, tsArr []tsdb.TimeSeries) propagateInterestSetMetricsMsg {
	aux := make([]body_types.Timeseries, len(tsArr))
	for idx, t := range tsArr {
		allPts := t.All()
		toAdd := body_types.Timeseries{
			Name:   t.Name(),
			Tags:   t.Tags(),
			Points: make([]body_types.Point, len(allPts)),
		}

		for idx, p := range allPts {
			toAdd.Points[idx] = body_types.Point{
				TS:     p.TS(),
				Fields: p.Value(),
			}
		}
		aux[idx] = toAdd
	}
	return propagateInterestSetMetricsMsg{
		InterestSetId: interestSetId,
		Metrics:       aux,
	}
}

func (propagateInterestSetMetricsMsg) Type() message.ID {
	return propagateInterestSetMetricsMsgID
}

func (propagateInterestSetMetricsMsg) Serializer() message.Serializer {
	return propagateInterestSetMetricsMsgSerializerVar
}

func (propagateInterestSetMetricsMsg) Deserializer() message.Deserializer {
	return propagateInterestSetMetricsMsgSerializerVar
}

type propagateInterestSetMetricsMsgSerializer struct{}

func (propagateInterestSetMetricsMsgSerializer) Serialize(m message.Message) []byte {
	mConverted := m.(propagateInterestSetMetricsMsg)
	msgBytes, err := json.Marshal(mConverted)
	if err != nil {
		panic(err)
	}
	return msgBytes
}

func (propagateInterestSetMetricsMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	toDeserialize := propagateInterestSetMetricsMsg{}
	err := json.Unmarshal(msgBytes, &toDeserialize)
	if err != nil {
		panic(err)
	}
	return toDeserialize
}
