package importer

import (
	"github.com/nm-morais/go-babel/pkg/message"
)

// -------------- Metric --------------

const metricMessageID = 100

type metricMessage struct {
}

func NewMetricMessage(metrics string) metricMessage {
	return metricMessage{}
}

func (metricMessage) Type() message.ID {
	return metricMessageID
}

func (metricMessage) Serializer() message.Serializer {
	return metricMsgSerializer
}

func (metricMessage) Deserializer() message.Deserializer {
	return metricMsgSerializer
}

var metricMsgSerializer = MetricMsgSerializer{}

type MetricMsgSerializer struct {
}

func (MetricMsgSerializer) Serialize(m message.Message) []byte { // can be optimized to spend less memory
	return []byte{}
}

func (MetricMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	return metricMessage{}
}
