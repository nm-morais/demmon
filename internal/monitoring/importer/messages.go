package importer

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	influxdb "github.com/influxdata/influxdb/client/v2"
	models "github.com/influxdata/influxdb/models"
	"github.com/nm-morais/go-babel/pkg/message"
)

// -------------- Metric --------------

const metricMessageID = 100

type metricMessage struct {
	Metrics influxdb.BatchPoints
}

func NewMetricMessage(metrics influxdb.BatchPoints) metricMessage {
	return metricMessage{
		Metrics: metrics,
	}
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
	buf := &bytes.Buffer{}
	metricMsg := m.(metricMessage)

	err := binary.Write(buf, binary.BigEndian, uint16(len([]byte(metricMsg.Metrics.Precision()))))
	if err != nil {
		panic(err)
	}

	buf.WriteString(metricMsg.Metrics.Precision())
	bp := metricMsg.Metrics
	for _, p := range metricMsg.Metrics.Points() {
		if p == nil {
			continue
		}

		if _, err := io.WriteString(buf, p.PrecisionString(bp.Precision())); err != nil {
			panic(err)
		}

		if _, err := buf.Write([]byte{'\n'}); err != nil {
			panic(err)
		}
	}
	return buf.Bytes()
}

func (MetricMsgSerializer) Deserialize(msgBytes []byte) message.Message {
	bufPos := 0
	precisionBytesNr := binary.BigEndian.Uint16(msgBytes)
	bufPos += 2

	precision := string(msgBytes[bufPos : uint16(bufPos)+precisionBytesNr])
	bufPos += int(precisionBytesNr)

	points, err := models.ParsePointsWithPrecision(msgBytes[bufPos:], time.Now().UTC(), precision)

	if err != nil {
		panic(err)
	}
	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{Precision: precision})
	if err != nil {
		panic(err)
	}

	for _, p := range points {
		f, err := p.Fields()
		if err != nil {
			panic(err)
		}

		p2, err := influxdb.NewPoint(string(p.Name()), p.Tags().Map(), f, p.Time())
		if err != nil {
			panic(err)
		}
		bp.AddPoint(p2)
	}
	return metricMessage{
		Metrics: bp,
	}
}
