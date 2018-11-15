package ftdc

import (
	"bytes"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/bsoncore"
	"github.com/pkg/errors"
)

type betterCollector struct {
	metadata   bson.Raw
	reference  bson.Raw
	startedAt  time.Time
	lastSample []int64
	deltas     []int64
	numSamples int
	maxDeltas  int
}

// NewBasicCollector provides a basic FTDC data collector that mirrors
// the server's implementation. The Add method will error if you
// attempt to add more than the specified number of records (plus one,
// as the reference/schema document doesn't count).
func NewBaseCollector(maxSize int) Collector {
	return &betterCollector{
		maxDeltas: maxSize,
	}
}

func (c *betterCollector) SetMetadata(doc bson.Raw) { c.metadata = doc }
func (c *betterCollector) Reset() {
	c.reference = nil
	c.lastSample = nil
	c.deltas = nil
	c.numSamples = 0
}

func (c *betterCollector) Info() CollectorInfo {
	var num int
	if c.reference != nil {
		num++
	}
	return CollectorInfo{
		SampleCount:  num + c.numSamples,
		MetricsCount: len(c.lastSample),
	}
}

func (c *betterCollector) Add(doc bson.Raw) error {
	if c.reference == nil {
		c.startedAt = time.Now()
		c.reference = doc
		metrics, err := extractMetricsFromDocument(doc)
		if err != nil {
			return errors.WithStack(err)
		}
		c.lastSample = metrics
		c.deltas = make([]int64, c.maxDeltas*len(c.lastSample))
		return nil
	}

	if c.numSamples >= c.maxDeltas {
		return errors.New("collector is overfull")
	}

	metrics, err := extractMetricsFromDocument(doc)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(metrics) != len(c.lastSample) {
		return errors.Errorf("unexpected schema change detected for sample %d: [current=%d vs previous=%d]",
			c.numSamples+1, len(metrics), len(c.lastSample),
		)
	}

	for idx := range metrics {
		c.deltas[getOffset(c.maxDeltas, c.numSamples, idx)] = metrics[idx] - c.lastSample[idx]
	}

	c.numSamples++
	c.lastSample = metrics

	return nil
}

func (c *betterCollector) Resolve() ([]byte, error) {
	if c.reference == nil {
		return nil, errors.New("no reference document")
	}

	data, err := c.getPayload()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := []byte{}
	if c.metadata != nil {
		doc := []byte{}
		doc = bsoncore.AppendTimeElement(doc, "_id", c.startedAt)
		doc = bsoncore.AppendInt32Element(doc, "type", 0)
		doc = bsoncore.AppendDocumentElement(doc, "doc", c.metadata)

		out = bsoncore.BuildDocument(out, doc)
	}

	doc := []byte{}
	doc = bsoncore.AppendTimeElement(doc, "_id", c.startedAt)
	doc = bsoncore.AppendInt32Element(doc, "type", 1)
	doc = bsoncore.AppendDocumentElement(doc, "data", data)

	return bsoncore.BuildDocument(out, doc), nil
}

func (c *betterCollector) getPayload() ([]byte, error) {
	payload := bytes.NewBuffer([]byte{})
	payload.Write(c.reference)
	payload.Write(encodeSizeValue(uint32(len(c.lastSample))))
	payload.Write(encodeSizeValue(uint32(c.numSamples)))

	zeroCount := int64(0)
	for i := 0; i < len(c.lastSample); i++ {
		for j := 0; j < c.numSamples; j++ {
			delta := c.deltas[getOffset(c.maxDeltas, j, i)]

			if delta == 0 {
				zeroCount++
				continue
			}

			if zeroCount > 0 {
				payload.Write(encodeValue(0))
				payload.Write(encodeValue(zeroCount - 1))
				zeroCount = 0
			}

			payload.Write(encodeValue(delta))
		}

		if i == len(c.lastSample)-1 && zeroCount > 0 {
			payload.Write(encodeValue(0))
			payload.Write(encodeValue(zeroCount - 1))
		}
	}

	if zeroCount > 0 {
		payload.Write(encodeValue(0))
		payload.Write(encodeValue(zeroCount - 1))
	}

	data, err := compressBuffer(payload.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "problem compressing payload")
	}

	return data, nil
}
