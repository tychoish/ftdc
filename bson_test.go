package ftdc

import (
	"strings"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/bsoncore"
	"github.com/mongodb/mongo-go-driver/bson/bsontype"
	"github.com/mongodb/mongo-go-driver/bson/decimal"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBSONValueToMetric(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		Name  string
		Value bson.RawValue
		Key   string
		Path  []string

		Expected  int64
		OutputLen int
	}{
		{
			Name: "ObjectID",
			Value: bson.RawValue{
				Type:  bsontype.ObjectID,
				Value: bsoncore.AppendObjectID([]byte{}, objectid.New()),
			},
		},
		{
			Name: "StringShort",
			Value: bson.RawValue{
				Type:  bsontype.String,
				Value: bsoncore.AppendString([]byte{}, "hello world"),
			},
		},
		{
			Name: "StringEmpty",
			Value: bson.RawValue{
				Type:  bsontype.String,
				Value: bsoncore.AppendString([]byte{}, ""),
			},
		},
		{
			Name: "StringLooksLikeNumber",
			Value: bson.RawValue{
				Type:  bsontype.String,
				Value: bsoncore.AppendString([]byte{}, "32"),
			},
		},
		{
			Name: "Decimal128Empty",
			Value: bson.RawValue{
				Type:  bsontype.Decimal128,
				Value: bsoncore.AppendDecimal128([]byte{}, decimal.Decimal128{}),
			},
		},
		{
			Name: "Decimal128",
			Value: bson.RawValue{
				Type:  bsontype.Decimal128,
				Value: bsoncore.AppendDecimal128([]byte{}, decimal.NewDecimal128(33, 43)),
			},
		},
		{
			Name: "DBPointer",
			Value: bson.RawValue{
				Type:  bsontype.DBPointer,
				Value: bsoncore.AppendDBPointer([]byte{}, "foo.bar", objectid.New()),
			},
		},
		{
			Name:      "BoolTrue",
			Path:      []string{"one", "two"},
			Key:       "foo",
			OutputLen: 1,
			Expected:  1,
			Value: bson.RawValue{
				Type:  bsontype.Boolean,
				Value: bsoncore.AppendBoolean([]byte{}, true),
			},
		},
		{
			Name:      "BoolFalse",
			Key:       "foo",
			Path:      []string{"one", "two"},
			OutputLen: 1,
			Expected:  0,
			Value: bson.RawValue{
				Type:  bsontype.Boolean,
				Value: bsoncore.AppendBoolean([]byte{}, false),
			},
		},
		{
			Name: "ArrayEmpty",
			Key:  "foo",
			Path: []string{"one", "two"},
			Value: bson.RawValue{
				Type:  bsontype.Array,
				Value: bsoncore.AppendArray([]byte{}, bsoncore.BuildDocument([]byte{}, []byte{})),
			},
		},
		{
			Name: "ArrayOfStrings",
			Key:  "foo",
			Path: []string{"one", "two"},
			Value: bson.RawValue{
				Type: bsontype.Array,
				Value: bsoncore.AppendArray([]byte{},
					bsoncore.AppendString(
						bsoncore.AppendString([]byte{}, "two"),
						"one")),
			},
		},
		{
			Name:      "ArrayOfMixed",
			Key:       "foo",
			Path:      []string{"one", "two"},
			OutputLen: 1,
			Expected:  1,
			Value: bson.RawValue{
				Type: bsontype.Array,
				Value: buildArray([]byte{},
					bsoncore.AppendStringElement([]byte{}, "1", "one"),
					bsoncore.AppendBooleanElement([]byte{}, "2", true)),
			},
		},
		{
			Name:      "ArrayOfBools",
			Key:       "foo",
			Path:      []string{"one", "two"},
			OutputLen: 2,
			Expected:  1,
			Value: bson.RawValue{
				Type: bsontype.Array,
				Value: buildArray([]byte{},
					bsoncore.AppendBooleanElement([]byte{}, "1", true),
					bsoncore.AppendBooleanElement([]byte{}, "2", true)),
			},
		},
		{
			Name: "EmptyDocument",
			Value: bson.RawValue{
				Type:  bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{}, []byte{}),
			},
		},
		{
			Name: "DocumentWithNonMetricFields",
			Value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{},
					bsoncore.AppendStringElement([]byte{}, "foo", "bar")),
			},
		},
		{
			Name: "DocumentWithOneValue",
			Value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{},
					bsoncore.AppendBooleanElement([]byte{}, "foo", true)),
			},
			Key:       "foo",
			Path:      []string{"exists"},
			OutputLen: 1,
			Expected:  1,
		},
		{
			Name:      "Double",
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
			Value: bson.RawValue{
				Type:  bsontype.Double,
				Value: bsoncore.AppendDouble([]byte{}, 42.42),
			},
		},
		{
			Name:      "OtherDouble",
			OutputLen: 1,
			Expected:  int64(42.0),
			Key:       "foo",
			Path:      []string{"really", "exists"},
			Value: bson.RawValue{
				Type:  bsontype.Double,
				Value: bsoncore.AppendDouble([]byte{}, 42.0),
			},
		},
		{
			Name:      "DoubleZero",
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
			Value: bson.RawValue{
				Type:  bsontype.Double,
				Value: bsoncore.AppendDouble([]byte{}, 0),
			},
		},
		{
			Name:      "ShortZero",
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
			Value: bson.RawValue{
				Type:  bsontype.Int32,
				Value: bsoncore.AppendInt32([]byte{}, 0),
			},
		},
		{
			Name:      "Short",
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
			Value: bson.RawValue{
				Type:  bsontype.Int32,
				Value: bsoncore.AppendInt32([]byte{}, 42),
			},
		},
		{
			Name: "Long",
			Value: bson.RawValue{
				Type:  bsontype.Int64,
				Value: bsoncore.AppendInt64([]byte{}, 42),
			},
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name: "LongZero",
			Value: bson.RawValue{
				Type:  bsontype.Int64,
				Value: bsoncore.AppendInt64([]byte{}, 0),
			},
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DatetimeZero",
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
			Value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendDateTime([]byte{}, 0),
			},
		},
		{
			Name:      "DatetimeLarge",
			OutputLen: 1,
			Expected:  epochMs(now),
			Key:       "foo",
			Path:      []string{"really", "exists"},
			Value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendTime([]byte{}, now),
			},
		},
		{
			Name:      "TimeStamp",
			OutputLen: 2,
			Expected:  100000,
			Key:       "foo",
			Path:      []string{"really", "exists"},
			Value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendTimestamp([]byte{}, 100, 100),
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			m := metricForType(test.Key, test.Path, test.Value)
			require.Len(t, m, test.OutputLen)

			if test.OutputLen > 0 {
				assert.Equal(t, test.Expected, m[0].startingValue)
				assert.True(t, strings.HasPrefix(m[0].KeyName, test.Key), "pre='%s', str='%s'", m[0].KeyName, test.Key)
				assert.True(t, strings.HasPrefix(m[0].Key(), strings.Join(test.Path, ".")))
			} else {
				assert.NotNil(t, m)
			}
		})
	}
}

func TestExtractingMetrics(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		Name              string
		Value             bson.RawValue
		ExpectedCount     int
		FirstEncodedValue int64
		NumEncodedValues  int
	}{
		{
			Name: "IgnoredType",
			Value: bson.RawValue{
				Type: bsontype.Null,
			},
			ExpectedCount:     0,
			FirstEncodedValue: 0,
			NumEncodedValues:  0,
		},
		{
			Name: "ObjectID",
			Value: bson.RawValue{
				Type:  bsontype.ObjectID,
				Value: bsoncore.AppendObjectID([]byte{}, objectid.New()),
			},
			ExpectedCount:     0,
			FirstEncodedValue: 0,
			NumEncodedValues:  0,
		},
		{
			Name: "String",
			Value: bson.RawValue{
				Type:  bsontype.String,
				Value: bsoncore.AppendString([]byte{}, "foo"),
			},
			ExpectedCount:     0,
			FirstEncodedValue: 0,
			NumEncodedValues:  0,
		},
		{
			Name: "Decimal128",
			Value: bson.RawValue{
				Type:  bsontype.Decimal128,
				Value: bsoncore.AppendDecimal128([]byte{}, decimal.NewDecimal128(33, 43)),
			},
			ExpectedCount:     0,
			FirstEncodedValue: 0,
			NumEncodedValues:  0,
		},
		{
			Name: "BoolTrue",
			Value: bson.RawValue{
				Type:  bsontype.Boolean,
				Value: bsoncore.AppendBoolean([]byte{}, true),
			},
			ExpectedCount:     1,
			FirstEncodedValue: 1,
			NumEncodedValues:  1,
		},
		{
			Name: "BoolFalse",
			Value: bson.RawValue{
				Type:  bsontype.Boolean,
				Value: bsoncore.AppendBoolean([]byte{}, false),
			},
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  1,
		},
		{
			Name: "Int32",
			Value: bson.RawValue{
				Type:  bsontype.Int32,
				Value: bsoncore.AppendInt32([]byte{}, 42),
			},
			ExpectedCount:     1,
			FirstEncodedValue: 42,
			NumEncodedValues:  1,
		},
		{
			Name: "Int32Zero",
			Value: bson.RawValue{
				Type:  bsontype.Int32,
				Value: bsoncore.AppendInt32([]byte{}, 0),
			},
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  1,
		},
		{
			Name: "Int32Negative",
			Value: bson.RawValue{
				Type:  bsontype.Int32,
				Value: bsoncore.AppendInt32([]byte{}, -42),
			},
			ExpectedCount:     1,
			FirstEncodedValue: -42,
			NumEncodedValues:  1,
		},
		{
			Name: "Int64",
			Value: bson.RawValue{
				Type:  bsontype.Int64,
				Value: bsoncore.AppendInt64([]byte{}, 42),
			},
			ExpectedCount:     1,
			FirstEncodedValue: 42,
			NumEncodedValues:  1,
		},
		{
			Name: "Int64Zero",
			Value: bson.RawValue{
				Type:  bsontype.Int64,
				Value: bsoncore.AppendInt64([]byte{}, 0),
			},
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  1,
		},
		{
			Name: "Int64Negative",
			Value: bson.RawValue{
				Type:  bsontype.Int64,
				Value: bsoncore.AppendInt64([]byte{}, -42),
			},
			ExpectedCount:     1,
			FirstEncodedValue: -42,
			NumEncodedValues:  1,
		},
		{
			Name: "DateTimeZero",
			Value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendDateTime([]byte{}, 0),
			},
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  1,
		},
		{
			Name: "DateTime",
			Value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendTime([]byte{}, now),
			},
			ExpectedCount:     1,
			FirstEncodedValue: epochMs(now),
			NumEncodedValues:  1,
		},
		{
			Name: "TimestampZero",
			Value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendTimestamp([]byte{}, 0, 0),
			},
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  2,
		},
		{
			Name: "TimestampLarger",
			Value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendTimestamp([]byte{}, 100, 100),
			},
			ExpectedCount:     1,
			FirstEncodedValue: 42,
			NumEncodedValues:  2,
		},
		{
			Name: "EmptyDocument",
			Value: bson.RawValue{
				Type:  bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{}, []byte{}),
			},
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name: "DoubleNoTruncate",
			Value: bson.RawValue{
				Type:  bsontype.Double,
				Value: bsoncore.AppendDouble([]byte{}, 42.0),
			},
			NumEncodedValues:  1,
			ExpectedCount:     1,
			FirstEncodedValue: 40,
		},
		{
			Name: "DoubleTruncate",
			Value: bson.RawValue{
				Type:  bsontype.Double,
				Value: bsoncore.AppendDouble([]byte{}, 40.20),
			},
			NumEncodedValues:  1,
			ExpectedCount:     1,
			FirstEncodedValue: 40,
		},
		{
			Name:              "SingleMetricValue",
			ExpectedCount:     1,
			NumEncodedValues:  1,
			FirstEncodedValue: 42,
			Value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{},
					bsoncore.AppendInt64Element([]byte{}, "foo", 42)),
			},
		},
		{
			Name:              "MultiMetricValue",
			ExpectedCount:     2,
			NumEncodedValues:  2,
			FirstEncodedValue: 7,
			Value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{},
					bsoncore.AppendInt64Element(bsoncore.AppendInt64Element([]byte{}, "foo", 72), "bar", 7)),
			},
		},
		{
			Name:              "MultiNonMetricValue",
			ExpectedCount:     0,
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
			Value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendStringElement(bsoncore.AppendStringElement([]byte{},
					"foo", "var"),
					"bar", "bar")),
			},
		},
		{
			Name:              "MixedArrayFirstMetrics",
			ExpectedCount:     2,
			NumEncodedValues:  2,
			FirstEncodedValue: 1,
			Value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendBooleanElement(bsoncore.AppendStringElement(bsoncore.AppendInt64Element([]byte{},
					"bar", 7),
					"foo", "var"),
					"zp", true)),
			},
		},
		{
			Name:              "ArrayEmptyArray",
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
			Value: bson.RawValue{
				Type:  bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, []byte{}),
			},
		},
		{
			Name:              "ArrayWithSingleMetricValue",
			ExpectedCount:     1,
			NumEncodedValues:  1,
			FirstEncodedValue: 42,
			Value: bson.RawValue{
				Type:  bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element([]byte{}, "0", 42)),
			},
		},
		{
			Name:              "ArrayWithMultiMetricValue",
			ExpectedCount:     2,
			NumEncodedValues:  2,
			FirstEncodedValue: 7,
			Value: bson.RawValue{
				Type: bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element(bsoncore.AppendInt64Element([]byte{},
					"1", 72),
					"0", 7)),
			},
		},
		{
			Name:              "ArrayWithMultiNonMetricValue",
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
			Value: bson.RawValue{
				Type: bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendStringElement(bsoncore.AppendStringElement([]byte{},
					"1", "foo"),
					"0", "bar")),
			},
		},
		{
			Name:              "ArrayWithMixedArrayFirstMetrics",
			NumEncodedValues:  2,
			ExpectedCount:     2,
			FirstEncodedValue: 1,
			Value: bson.RawValue{
				Type: bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendBooleanElement(bsoncore.AppendStringElement(bsoncore.AppendInt64Element([]byte{},
					"2", 7),
					"1", "var"),
					"0", true)),
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			metrics, err := extractMetricsFromValue(test.Value)
			assert.NoError(t, err)
			assert.Equal(t, test.NumEncodedValues, len(metrics))

			keys, num := isMetricsValue("keyname", test.Value)
			if test.NumEncodedValues > 0 {
				assert.Equal(t, test.FirstEncodedValue, metrics[0])
				assert.True(t, len(keys) >= 1)
				assert.True(t, strings.HasPrefix(keys[0], "keyname"))
			} else {
				assert.Len(t, keys, 0)
				assert.Zero(t, num)
			}

		})
	}
}

func TestDocumentExtraction(t *testing.T) {
	for _, test := range []struct {
		Name               string
		Document           bson.Raw
		EncoderShouldError bool
		NumEncodedValues   int
		FirstEncodedValue  int64
	}{
		{
			Name:              "EmptyDocument",
			Document:          bsoncore.BuildDocument([]byte{}, []byte{}),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "NilDocumentsDocument",
			Document:          bson.Raw{},
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "SingleMetricValue",
			Document:          bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element([]byte{}, "foo", 42)),
			NumEncodedValues:  1,
			FirstEncodedValue: 42,
		},
		{
			Name:              "MultiMetricValue",
			Document:          bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element([]byte{}, "foo", 42)),
			NumEncodedValues:  2,
			FirstEncodedValue: 7,
		},
		{
			Name: "MultiNonMetricValue",
			Document: bsoncore.BuildDocument([]byte{}, bsoncore.AppendStringElement(bsoncore.AppendStringElement([]byte{},
				"bar", "bar"),
				"foo", "var")),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name: "MixedArrayFirstMetrics",
			Document: bsoncore.BuildDocument([]byte{}, bsoncore.AppendBooleanElement(bsoncore.AppendStringElement(bsoncore.AppendInt64Element([]byte{},
				"2", 7),
				"1", "var"),
				"0", true)),
			NumEncodedValues:  2,
			FirstEncodedValue: 1,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			metrics, err := extractMetricsFromDocument(test.Document)
			assert.NoError(t, err)
			assert.Equal(t, test.NumEncodedValues, len(metrics))
			if len(metrics) > 0 {
				assert.Equal(t, test.FirstEncodedValue, metrics[0])
			}
		})
	}
}

func TestMetricsHashValue(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name        string
		value       bson.RawValue
		expectedNum int
		keyElems    int
	}{
		{
			name: "IgnoredType",
			value: bson.RawValue{
				Type: bsontype.Null,
			},
			keyElems:    0,
			expectedNum: 0,
		},
		{
			name: "ObjectID",
			value: bson.RawValue{
				Type:  bsontype.ObjectID,
				Value: bsoncore.AppendObjectID([]byte{}, objectid.New()),
			},
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name: "String",
			value: bson.RawValue{
				Type:  bsontype.String,
				Value: bsoncore.AppendString([]byte{}, "foo"),
			},
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name: "Decimal128",
			value: bson.RawValue{
				Type:  bsontype.Decimal128,
				Value: bsoncore.AppendDecimal128([]byte{}, decimal.NewDecimal128(33, 43)),
			},
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name: "BoolTrue",
			value: bson.RawValue{
				Type:  bsontype.Boolean,
				Value: bsoncore.AppendBoolean([]byte{}, true),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "BoolFalse",
			value: bson.RawValue{
				Type:  bsontype.Boolean,
				Value: bsoncore.AppendBoolean([]byte{}, false),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "Int32",
			value: bson.RawValue{
				Type:  bsontype.Int32,
				Value: bsoncore.AppendInt32([]byte{}, 42),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "Int32Zero",
			value: bson.RawValue{
				Type:  bsontype.Int32,
				Value: bsoncore.AppendInt32([]byte{}, 0),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "Int32Negative",
			value: bson.RawValue{
				Type:  bsontype.Int32,
				Value: bsoncore.AppendInt32([]byte{}, -42),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "Int64",
			value: bson.RawValue{
				Type:  bsontype.Int64,
				Value: bsoncore.AppendInt64([]byte{}, 42),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "Int64Zero",
			value: bson.RawValue{
				Type:  bsontype.Int64,
				Value: bsoncore.AppendInt64([]byte{}, 0),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "Int64Negative",
			value: bson.RawValue{
				Type:  bsontype.Int64,
				Value: bsoncore.AppendInt64([]byte{}, -42),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "DateTimeZero",
			value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendDateTime([]byte{}, 0),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "DateTime",
			value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendTime([]byte{}, now),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "TimestampZero",
			value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendTimestamp([]byte{}, 0, 0),
			},
			expectedNum: 2,
			keyElems:    1,
		},
		{
			name: "TimestampLarger",
			value: bson.RawValue{
				Type:  bsontype.DateTime,
				Value: bsoncore.AppendTimestamp([]byte{}, 100, 100),
			},
			expectedNum: 2,
			keyElems:    1,
		},
		{
			name: "EmptyDocument",
			value: bson.RawValue{
				Type:  bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{}, []byte{}),
			},
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name: "SingleMetricValue",
			value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{},
					bsoncore.AppendInt64Element([]byte{}, "foo", 42)),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "MultiMetricValue",
			value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{},
					bsoncore.AppendInt64Element(bsoncore.AppendInt64Element([]byte{}, "foo", 72), "bar", 7)),
			},
			expectedNum: 2,
			keyElems:    2,
		},
		{
			name: "MultiNonMetricValue",
			value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendStringElement(bsoncore.AppendStringElement([]byte{},
					"foo", "var"),
					"bar", "bar")),
			},
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name: "MixedArrayFirstMetrics",
			value: bson.RawValue{
				Type: bsontype.EmbeddedDocument,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendBooleanElement(bsoncore.AppendStringElement(bsoncore.AppendInt64Element([]byte{},
					"bar", 7),
					"foo", "var"),
					"zp", true)),
			},
			expectedNum: 2,
			keyElems:    2,
		},
		{
			name: "ArraEmptyArray",
			value: bson.RawValue{
				Type:  bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, []byte{}),
			},
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name: "ArrayWithSingleMetricValue",
			value: bson.RawValue{
				Type:  bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element([]byte{}, "0", 42)),
			},
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name: "ArrayWithMultiMetricValue",
			value: bson.RawValue{
				Type: bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element(bsoncore.AppendInt64Element([]byte{},
					"1", 72),
					"0", 7)),
			},
			expectedNum: 2,
			keyElems:    2,
		},
		{
			name: "ArrayWithMultiNonMetricValue",
			value: bson.RawValue{
				Type: bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendStringElement(bsoncore.AppendStringElement([]byte{},
					"1", "foo"),
					"0", "bar")),
			},
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name: "ArrayWithMixedArrayFirstMetrics",
			value: bson.RawValue{
				Type: bsontype.Array,
				Value: bsoncore.BuildDocument([]byte{}, bsoncore.AppendBooleanElement(bsoncore.AppendStringElement(bsoncore.AppendInt64Element([]byte{},
					"2", 7),
					"1", "var"),
					"0", true)),
			},
			expectedNum: 2,
			keyElems:    2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys, num := isMetricsValue("key", test.value)
			assert.Equal(t, test.expectedNum, num)
			assert.Equal(t, test.keyElems, len(keys))
		})
	}
}

// func TestMetricsToElement(t *testing.T) {
// 	for _, test := range []struct {
// 		name       string
// 		ref        bson.RawElement
// 		metrics    []Metric
// 		expected   bson.RawElement
// 		outNum     int
// 		isDocument bool
// 	}{
// 		{
// 			name: "ObjectID",
// 			ref:  bson.EC.ObjectID("foo", objectid.New()),
// 		},
// 		{
// 			name: "String",
// 			ref:  bson.EC.String("foo", "bar"),
// 		},
// 		{
// 			name: "Regex",
// 			ref:  bson.EC.Regex("foo", "bar", "bar"),
// 		},
// 		{
// 			name: "Decimal128",
// 			ref:  bson.EC.Decimal128("foo", decimal.NewDecimal128(1, 2)),
// 		},
// 		{
// 			name: "Double",
// 			ref:  bson.EC.Double("foo", 4.42),
// 			metrics: []Metric{
// 				{Values: []int64{4}},
// 			},
// 			expected: bson.EC.Double("foo", 4.0),
// 			outNum:   1,
// 		},
// 		{
// 			name: "Short",
// 			ref:  bson.EC.Int32("foo", 4),
// 			metrics: []Metric{
// 				{Values: []int64{37}},
// 			},
// 			expected: bson.EC.Int32("foo", 37),
// 			outNum:   1,
// 		},
// 		{

// 			name: "FalseBool",
// 			ref:  bson.EC.Boolean("foo", true),
// 			metrics: []Metric{
// 				{Values: []int64{0}},
// 			},
// 			expected: bson.EC.Boolean("foo", false),
// 			outNum:   1,
// 		},
// 		{

// 			name: "TrueBool",
// 			ref:  bson.EC.Boolean("foo", false),
// 			metrics: []Metric{
// 				{Values: []int64{1}},
// 			},
// 			expected: bson.EC.Boolean("foo", true),
// 			outNum:   1,
// 		},
// 		{

// 			name: "SuperTrueBool",
// 			ref:  bson.EC.Boolean("foo", false),
// 			metrics: []Metric{
// 				{Values: []int64{100}},
// 			},
// 			expected: bson.EC.Boolean("foo", true),
// 			outNum:   1,
// 		},
// 		{

// 			name:       "EmptyDocument",
// 			ref:        bson.EC.SubDocument("foo", bson.NewDocument()),
// 			expected:   bson.EC.SubDocument("foo", bson.NewDocument()),
// 			isDocument: true,
// 		},
// 		{

// 			name: "DateTimeFromTime",
// 			ref:  bson.EC.Time("foo", time.Now()),
// 			metrics: []Metric{
// 				{Values: []int64{1000}},
// 			},
// 			expected: bson.EC.DateTime("foo", 1000),
// 			outNum:   1,
// 		},
// 		{

// 			name: "DateTime",
// 			ref:  bson.EC.DateTime("foo", 19999),
// 			metrics: []Metric{
// 				{Values: []int64{1000}},
// 			},
// 			expected: bson.EC.DateTime("foo", 1000),
// 			outNum:   1,
// 		},
// 		{

// 			name: "TimeStamp",
// 			ref:  bson.EC.Timestamp("foo", 19999, 100),
// 			metrics: []Metric{
// 				{Values: []int64{1000}},
// 				{Values: []int64{1000}},
// 			},
// 			expected: bson.EC.Timestamp("foo", 1000, 1000),
// 			outNum:   2,
// 		},
// 		{
// 			name:     "ArrayEmpty",
// 			ref:      bson.EC.ArrayFromElements("foo", bson.VC.String("foo"), bson.VC.String("bar")),
// 			expected: bson.EC.Array("foo", bson.NewArray()),
// 		},
// 		{
// 			name: "ArraySingle",
// 			metrics: []Metric{
// 				{Values: []int64{1}},
// 			},
// 			ref:      bson.EC.ArrayFromElements("foo", bson.VC.Boolean(true)),
// 			expected: bson.EC.Array("foo", bson.NewArray(bson.VC.Boolean(true))),
// 			outNum:   1,
// 		},
// 		{
// 			name: "ArrayMulti",
// 			metrics: []Metric{
// 				{Values: []int64{1}},
// 				{Values: []int64{77}},
// 			},
// 			ref:      bson.EC.ArrayFromElements("foo", bson.VC.Boolean(true), bson.VC.Int32(33)),
// 			expected: bson.EC.Array("foo", bson.NewArray(bson.VC.Boolean(true), bson.VC.Int32(77))),
// 			outNum:   2,
// 		},
// 	} {
// 		t.Run(test.name, func(t *testing.T) {
// 			elem, num := rehydrateElement(test.ref, 0, test.metrics, 0)
// 			assert.Equal(t, test.outNum, num)
// 			if !test.isDocument {
// 				assert.Equal(t, test.expected, elem)
// 			} else {
// 				assert.True(t, test.expected.Value().MutableDocument().Equal(elem.Value().MutableDocument()))
// 			}

// 		})
// 	}
// }

func TestIsOneChecker(t *testing.T) {
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.Int32, Value: bsoncore.AppendInt32([]byte{}, 32)}))
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.Int32, Value: bsoncore.AppendInt32([]byte{}, 0)}))
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.Int64, Value: bsoncore.AppendInt64([]byte{}, 32)}))
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.Int64, Value: bsoncore.AppendInt64([]byte{}, 0)}))
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.Double, Value: bsoncore.AppendDouble([]byte{}, 32.2)}))
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.Double, Value: bsoncore.AppendDouble([]byte{}, 0.45)}))
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.Double, Value: bsoncore.AppendDouble([]byte{}, 0.0)}))
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.String, Value: bsoncore.AppendString([]byte{}, "foo")}))
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.Boolean, Value: bsoncore.AppendBoolean([]byte{}, true)}))
	assert.False(t, isNum(1, bson.RawValue{Type: bsontype.Boolean, Value: bsoncore.AppendBoolean([]byte{}, false)}))

	assert.True(t, isNum(1, bson.RawValue{Type: bsontype.Int32, Value: bsoncore.AppendInt32([]byte{}, 1)}))
	assert.True(t, isNum(1, bson.RawValue{Type: bsontype.Int64, Value: bsoncore.AppendInt64([]byte{}, 1)}))
	assert.True(t, isNum(1, bson.RawValue{Type: bsontype.Double, Value: bsoncore.AppendDouble([]byte{}, 1.0)}))
}
