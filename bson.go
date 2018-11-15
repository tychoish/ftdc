package ftdc

import (
	"fmt"
	"strings"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/bsoncore"
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////
//
// Helpers for parsing the timeseries data from a metrics payload

func metricForDocument(path []string, d bson.Raw) ([]Metric, error) {
	elems, err := d.Elements()
	if err != nil {
		return nil, errors.Wrap(err, "problem parsing bson")
	}

	o := []Metric{}
	for _, e := range elems {
		o = append(o, metricForType(e.Key(), path, e.Value())...)
	}

	return o, nil
}

func metricForType(key string, path []string, val bson.RawValue) []Metric {
	switch val.Type {
	case bson.TypeObjectID:
		return []Metric{}
	case bson.TypeString:
		return []Metric{}
	case bson.TypeDecimal128:
		return []Metric{}
	case bson.TypeArray:
		path = append(path, key)
		out, err := metricForDocument(path, val.Array()) // notlint
		if err != nil {
			panic(err)
		}
		return out
	case bson.TypeEmbeddedDocument:
		path = append(path, key)

		o := []Metric{}
		m, _ := metricForDocument(path, val.Document()) // notlint
		for _, ne := range m {
			o = append(o, Metric{
				ParentPath:    path,
				KeyName:       ne.KeyName,
				startingValue: ne.startingValue,
				originalType:  ne.originalType,
			})
		}
		return o
	case bson.TypeBoolean:
		if val.Boolean() {
			return []Metric{
				{
					ParentPath:    path,
					KeyName:       key,
					startingValue: 1,
					originalType:  val.Type,
				},
			}
		}
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: 0,
				originalType:  val.Type,
			},
		}
	case bson.TypeDouble:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(val.Double()),
				originalType:  val.Type,
			},
		}
	case bson.TypeInt32:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(val.Int32()),
				originalType:  val.Type,
			},
		}
	case bson.TypeInt64:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: val.Int64(),
				originalType:  val.Type,
			},
		}
	case bson.TypeDateTime:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: epochMs(val.Time()),
				originalType:  val.Type,
			},
		}
	case bson.TypeTimestamp:
		t, i := val.Timestamp()
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(t) * 1000,
				originalType:  val.Type,
			},
			{
				ParentPath:    path,
				KeyName:       key + ".inc",
				startingValue: int64(i),
				originalType:  val.Type,
			},
		}
	default:
		return []Metric{}
	}
}

////////////////////////////////////////////////////////////////////////
//
// Processores use to return rich (i.e. non-flat) structures from
// metrics slices

func rehydrateDocument(ref bson.Raw, sample int, metrics []Metric, idx int) ([]byte, int) {
	if ref == nil {
		return nil, 0
	}

	elems, err := ref.Elements()
	if err != nil {
		return nil, 0
	}

	doc := []byte{}
	for _, elem := range elems {
		var out []byte
		out, idx = rehydrateElement(elem, sample, metrics, idx)
		doc = append(doc, out...)

	}

	return bsoncore.BuildDocument([]byte{}, doc), idx
}

func rehydrateElement(ref bson.RawElement, sample int, metrics []Metric, idx int) (bson.RawElement, int) {
	val := ref.Value()
	switch val.Type {
	case bson.TypeObjectID:
		return nil, idx
	case bson.TypeString:
		return nil, idx
	case bson.TypeDecimal128:
		return nil, idx
	case bson.TypeArray:
		elems, err := val.Array().Elements()
		if err != nil {
			return nil, idx
		}

		out := []byte{}
		for _, elem := range elems {
			var item bson.RawElement
			item, idx = rehydrateElement(elem, sample, metrics, idx)
			if item == nil {
				continue
			}
			out = append(out, item...)
		}

		return bsoncore.AppendArrayElement([]byte{}, ref.Key(), out), idx
	case bson.TypeEmbeddedDocument:
		doc, idx := rehydrateDocument(val.Document(), sample, metrics, idx)
		return bsoncore.AppendDocumentElement([]byte{}, ref.Key(), doc), idx
	case bson.TypeBoolean:
		value := metrics[idx].Values[sample]
		if value == 0 {
			return bsoncore.AppendBooleanElement([]byte{}, ref.Key(), false), idx + 1
		}
		return bsoncore.AppendBooleanElement([]byte{}, ref.Key(), true), idx + 1

	case bson.TypeDouble:
		return bsoncore.AppendDoubleElement([]byte{}, ref.Key(), float64(metrics[idx].Values[sample])), idx + 1
	case bson.TypeInt32:
		return bsoncore.AppendInt32Element([]byte{}, ref.Key(), int32(metrics[idx].Values[sample])), idx + 1
	case bson.TypeInt64:
		return bsoncore.AppendInt64Element([]byte{}, ref.Key(), metrics[idx].Values[sample]), idx + 1
	case bson.TypeDateTime:
		return bsoncore.AppendTimeElement([]byte{}, ref.Key(), timeEpocMs(metrics[idx].Values[sample])), idx + 1
	case bson.TypeTimestamp:
		return bsoncore.AppendTimestampElement([]byte{}, ref.Key(), uint32(metrics[idx].Values[sample]), uint32(metrics[idx+1].Values[sample])), idx + 2
	default:
		return nil, idx
	}
}

////////////////////////////////////////////////////////////////////////
//
// Helpers for encoding values from bson documents

func extractMetricsFromDocument(doc bson.Raw) ([]int64, error) {
	elems, err := doc.Elements()
	if err != nil {
		return nil, errors.Wrap(err, "problem parsing bson")
	}

	var (
		data    []int64
		metrics []int64
	)

	catcher := grip.NewBasicCatcher()

	for _, elem := range elems {
		data, err = extractMetricsFromValue(elem.Value())
		catcher.Add(err)
		metrics = append(metrics, data...)
	}

	return metrics, catcher.Resolve()
}

func extractMetricsFromValue(val bson.RawValue) ([]int64, error) {
	btype := val.Type
	switch btype {
	case bson.TypeObjectID:
		return nil, nil
	case bson.TypeString:
		return nil, nil
	case bson.TypeDecimal128:
		return nil, nil
	case bson.TypeArray:
		metrics, err := extractMetricsFromDocument(val.Array())
		return metrics, errors.WithStack(err)
	case bson.TypeEmbeddedDocument:
		metrics, err := extractMetricsFromDocument(val.Document())
		return metrics, errors.WithStack(err)
	case bson.TypeBoolean:
		if val.Boolean() {
			return []int64{1}, nil
		}
		return []int64{0}, nil
	case bson.TypeDouble:
		return []int64{int64(val.Double())}, nil
	case bson.TypeInt32:
		return []int64{int64(val.Int32())}, nil
	case bson.TypeInt64:
		return []int64{val.Int64()}, nil
	case bson.TypeDateTime:
		return []int64{epochMs(val.Time())}, nil
	case bson.TypeTimestamp:
		t, i := val.Timestamp()
		return []int64{int64(t), int64(i)}, nil
	default:
		return nil, nil
	}
}

////////////////////////////////////////////////////////////////////////
//
// hashing functions for metrics-able documents

func metricsHash(doc bson.Raw) (string, int) {
	keys, num := isMetricsDocument("", doc)
	return strings.Join(keys, "\n"), num
}

func isMetricsDocument(key string, doc bson.Raw) ([]string, int) {
	elems, err := doc.Elements()
	if err != nil {
		return nil, 0
	}

	keys := []string{}
	seen := 0
	for _, elem := range elems {
		k, num := isMetricsValue(fmt.Sprintf("%s/%s", key, elem.Key()), elem.Value())
		if num > 0 {
			seen += num
			keys = append(keys, k...)
		}
	}

	return keys, seen
}

func isMetricsValue(key string, val bson.RawValue) ([]string, int) {
	switch val.Type {
	case bson.TypeObjectID:
		return nil, 0
	case bson.TypeString:
		return nil, 0
	case bson.TypeDecimal128:
		return nil, 0
	case bson.TypeArray:
		return isMetricsDocument(key, val.Array())
	case bson.TypeEmbeddedDocument:
		return isMetricsDocument(key, val.Document())
	case bson.TypeBoolean:
		return []string{key}, 1
	case bson.TypeDouble:
		return []string{key}, 1
	case bson.TypeInt32:
		return []string{key}, 1
	case bson.TypeInt64:
		return []string{key}, 1
	case bson.TypeDateTime:
		return []string{key}, 1
	case bson.TypeTimestamp:
		return []string{key}, 2
	default:
		return nil, 0
	}
}

////////////////////////////////////////////////////////////////////////
//
// utility functions

func isNum(num int, val bson.RawValue) bool {
	switch val.Type {
	case bson.TypeInt32:
		return val.Int32() == int32(num)
	case bson.TypeInt64:
		return val.Int64() == int64(num)
	case bson.TypeDouble:
		return val.Double() == float64(num)
	default:
		return false
	}
}

func epochMs(t time.Time) int64 {
	return t.UnixNano() / 1000000
}

func timeEpocMs(in int64) time.Time {
	return time.Unix(int64(in)/1000, int64(in)%1000*1000000)
}
