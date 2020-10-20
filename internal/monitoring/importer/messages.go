package importer

import "github.com/nm-morais/go-babel/pkg/message"

const metricMessageID = 3000

type MetricsMessage struct {
	Metrics []byte
}

func NewMetricMessage(metrics []byte) MetricsMessage {
	return MetricsMessage{
		Metrics: metrics,
	}
}

func (MetricsMessage) Type() message.ID {
	return metricMessageID
}

func (MetricsMessage) Serializer() message.Serializer {
	return metricMsgSerializer
}

func (MetricsMessage) Deserializer() message.Deserializer {
	return metricMsgSerializer
}

var metricMsgSerializer = MetricMsgSerializer{}

type MetricMsgSerializer struct {
}

func (MetricMsgSerializer) Serialize(m message.Message) []byte { // can be optimized to spend less memory
	return m.(MetricsMessage).Metrics
}

func (MetricMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	return MetricsMessage{
		Metrics: msgBytes,
	}
}
