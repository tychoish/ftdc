package ftdc

import (
	"strings"
	"testing"
	"time"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/ftdc/bsonx/decimal"
	"github.com/mongodb/ftdc/bsonx/objectid"
	"github.com/stretchr/testify/assert"
)

func TestFlattenArray(t *testing.T) {
	t.Run("NilArray", func(t *testing.T) {
		out := metricForArray("", nil, nil)
		assert.NotNil(t, out)
		assert.Len(t, out, 0)
	})
	t.Run("EmptyArray", func(t *testing.T) {
		out := metricForArray("", nil, bsonx.NewArray())
		assert.NotNil(t, out)
		assert.Len(t, out, 0)
	})
	t.Run("TwoElements", func(t *testing.T) {
		m := metricForArray("foo", nil, bsonx.NewArray(bsonx.VC.Boolean(true), bsonx.VC.Boolean(false)))
		assert.NotNil(t, m)
		assert.Len(t, m, 2)

		assert.Equal(t, m[0].Key(), "foo.0")
		assert.Equal(t, m[1].Key(), "foo.1")
		assert.Equal(t, int64(1), m[0].startingValue)
		assert.Equal(t, int64(0), m[1].startingValue)
	})
	t.Run("TwoElementsWithSkippedValue", func(t *testing.T) {
		m := metricForArray("foo", nil, bsonx.NewArray(bsonx.VC.String("foo"), bsonx.VC.Boolean(false)))
		assert.NotNil(t, m)
		assert.Len(t, m, 1)

		assert.Equal(t, m[0].Key(), "foo.1")
		assert.Equal(t, int64(0), m[0].startingValue)
	})
	t.Run("ArrayWithOnlyStrings", func(t *testing.T) {
		out := metricForArray("foo", nil, bsonx.NewArray(bsonx.VC.String("foo"), bsonx.VC.String("bar")))
		assert.NotNil(t, out)
		assert.Len(t, out, 0)
	})
}

func TestBSONValueToMetric(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		Name  string
		Value *bsonx.Value
		Key   string
		Path  []string

		Expected  int64
		OutputLen int
	}{
		{
			Name:  "ObjectID",
			Value: bsonx.VC.ObjectID(objectid.New()),
		},
		{
			Name:  "StringShort",
			Value: bsonx.VC.String("Hello World"),
		},
		{
			Name:  "StringEmpty",
			Value: bsonx.VC.String(""),
		},
		{
			Name:  "StringLooksLikeNumber",
			Value: bsonx.VC.String("42"),
		},
		{
			Name:  "Decimal128Empty",
			Value: bsonx.VC.Decimal128(decimal.Decimal128{}),
		},
		{
			Name:  "Decimal128",
			Value: bsonx.VC.Decimal128(decimal.NewDecimal128(33, 43)),
		},
		{
			Name:  "DBPointer",
			Value: bsonx.VC.DBPointer("foo.bar", objectid.New()),
		},
		{
			Name:      "BoolTrue",
			Path:      []string{"one", "two"},
			Key:       "foo",
			Value:     bsonx.VC.Boolean(true),
			OutputLen: 1,
			Expected:  1,
		},
		{
			Name:      "BoolFalse",
			Key:       "foo",
			Path:      []string{"one", "two"},
			Value:     bsonx.VC.Boolean(false),
			OutputLen: 1,
			Expected:  0,
		},
		{
			Name:  "ArrayEmpty",
			Key:   "foo",
			Path:  []string{"one", "two"},
			Value: bsonx.VC.ArrayFromValues(),
		},
		{
			Name:  "ArrayOfStrings",
			Key:   "foo",
			Path:  []string{"one", "two"},
			Value: bsonx.VC.ArrayFromValues(bsonx.VC.String("one"), bsonx.VC.String("two")),
		},
		{
			Name:      "ArrayOfMixed",
			Key:       "foo",
			Path:      []string{"one", "two"},
			Value:     bsonx.VC.ArrayFromValues(bsonx.VC.String("one"), bsonx.VC.Boolean(true)),
			OutputLen: 1,
			Expected:  1,
		},
		{
			Name:      "ArrayOfBools",
			Key:       "foo",
			Path:      []string{"one", "two"},
			Value:     bsonx.VC.ArrayFromValues(bsonx.VC.Boolean(true), bsonx.VC.Boolean(true)),
			OutputLen: 2,
			Expected:  1,
		},
		{
			Name:  "EmptyDocument",
			Value: bsonx.VC.DocumentFromElements(),
		},
		{
			Name:  "DocumentWithNonMetricFields",
			Value: bsonx.VC.DocumentFromElements(bsonx.EC.String("foo", "bar")),
		},
		{
			Name:      "DocumentWithOneValue",
			Value:     bsonx.VC.DocumentFromElements(bsonx.EC.Boolean("foo", true)),
			Key:       "foo",
			Path:      []string{"exists"},
			OutputLen: 1,
			Expected:  1,
		},
		{
			Name:      "Double",
			Value:     bsonx.VC.Double(42.42),
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "OtherDouble",
			Value:     bsonx.VC.Double(42.0),
			OutputLen: 1,
			Expected:  int64(42.0),
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleZero",
			Value:     bsonx.VC.Double(0),
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleShortZero",
			Value:     bsonx.VC.Int32(0),
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleShort",
			Value:     bsonx.VC.Int32(42),
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleLong",
			Value:     bsonx.VC.Int64(42),
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleLongZero",
			Value:     bsonx.VC.Int64(0),
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DatetimeZero",
			Value:     bsonx.VC.DateTime(0),
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DatetimeLarge",
			Value:     bsonx.EC.Time("", now).Value(),
			OutputLen: 1,
			Expected:  epochMs(now),
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "TimeStamp",
			Value:     bsonx.VC.Timestamp(100, 100),
			OutputLen: 2,
			Expected:  100000,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			m := metricForType(test.Key, test.Path, test.Value)
			assert.Len(t, m, test.OutputLen)

			if test.OutputLen > 0 {
				assert.Equal(t, test.Expected, m[0].startingValue)
				assert.True(t, strings.HasPrefix(m[0].KeyName, test.Key))
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
		Value             *bsonx.Value
		ExpectedCount     int
		FirstEncodedValue int64
		NumEncodedValues  int
	}{
		{
			Name:              "IgnoredType",
			Value:             bsonx.VC.Null(),
			ExpectedCount:     0,
			FirstEncodedValue: 0,
			NumEncodedValues:  0,
		},
		{
			Name:              "ObjectID",
			Value:             bsonx.VC.ObjectID(objectid.New()),
			ExpectedCount:     0,
			FirstEncodedValue: 0,
			NumEncodedValues:  0,
		},
		{
			Name:              "String",
			Value:             bsonx.VC.String("foo"),
			ExpectedCount:     0,
			FirstEncodedValue: 0,
			NumEncodedValues:  0,
		},
		{
			Name:              "Decimal128",
			Value:             bsonx.VC.Decimal128(decimal.NewDecimal128(42, 42)),
			ExpectedCount:     0,
			FirstEncodedValue: 0,
			NumEncodedValues:  0,
		},
		{
			Name:              "BoolTrue",
			Value:             bsonx.VC.Boolean(true),
			ExpectedCount:     1,
			FirstEncodedValue: 1,
			NumEncodedValues:  1,
		},
		{
			Name:              "BoolFalse",
			Value:             bsonx.VC.Boolean(false),
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  1,
		},
		{
			Name:              "Int32",
			Value:             bsonx.VC.Int32(42),
			ExpectedCount:     1,
			FirstEncodedValue: 42,
			NumEncodedValues:  1,
		},
		{
			Name:              "Int32Zero",
			Value:             bsonx.VC.Int32(0),
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  1,
		},
		{
			Name:              "Int32Negative",
			Value:             bsonx.VC.Int32(-42),
			ExpectedCount:     1,
			FirstEncodedValue: -42,
			NumEncodedValues:  1,
		},
		{
			Name:              "Int64",
			Value:             bsonx.VC.Int64(42),
			ExpectedCount:     1,
			FirstEncodedValue: 42,
			NumEncodedValues:  1,
		},
		{
			Name:              "Int64Zero",
			Value:             bsonx.VC.Int64(0),
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  1,
		},
		{
			Name:              "Int64Negative",
			Value:             bsonx.VC.Int64(-42),
			ExpectedCount:     1,
			FirstEncodedValue: -42,
			NumEncodedValues:  1,
		},
		{
			Name:              "DateTimeZero",
			Value:             bsonx.VC.DateTime(0),
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  1,
		},
		{
			Name:              "TimestampZero",
			Value:             bsonx.VC.Timestamp(0, 0),
			ExpectedCount:     1,
			FirstEncodedValue: 0,
			NumEncodedValues:  2,
		},
		{
			Name:              "TimestampLarger",
			Value:             bsonx.VC.Timestamp(42, 42),
			ExpectedCount:     1,
			FirstEncodedValue: 42,
			NumEncodedValues:  2,
		},
		{
			Name:              "EmptyDocument",
			Value:             bsonx.EC.SubDocumentFromElements("data").Value(),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "SingleMetricValue",
			Value:             bsonx.EC.SubDocumentFromElements("data", bsonx.EC.Int64("foo", 42)).Value(),
			ExpectedCount:     1,
			NumEncodedValues:  1,
			FirstEncodedValue: 42,
		},
		{
			Name:              "MultiMetricValue",
			Value:             bsonx.EC.SubDocumentFromElements("data", bsonx.EC.Int64("foo", 7), bsonx.EC.Int32("foo", 72)).Value(),
			ExpectedCount:     2,
			NumEncodedValues:  2,
			FirstEncodedValue: 7,
		},
		{
			Name:              "MultiNonMetricValue",
			Value:             bsonx.EC.SubDocumentFromElements("data", bsonx.EC.String("foo", "var"), bsonx.EC.String("bar", "bar")).Value(),
			ExpectedCount:     0,
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "MixedArrayFirstMetrics",
			Value:             bsonx.EC.SubDocumentFromElements("data", bsonx.EC.Boolean("zp", true), bsonx.EC.String("foo", "var"), bsonx.EC.Int64("bar", 7)).Value(),
			ExpectedCount:     2,
			NumEncodedValues:  2,
			FirstEncodedValue: 1,
		},
		{
			Name:              "ArraEmptyArray",
			Value:             bsonx.VC.Array(bsonx.NewArray()),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "ArrayWithSingleMetricValue",
			Value:             bsonx.VC.ArrayFromValues(bsonx.VC.Int64(42)),
			ExpectedCount:     1,
			NumEncodedValues:  1,
			FirstEncodedValue: 42,
		},
		{
			Name:              "ArrayWithMultiMetricValue",
			Value:             bsonx.VC.ArrayFromValues(bsonx.VC.Int64(7), bsonx.VC.Int32(72)),
			ExpectedCount:     2,
			NumEncodedValues:  2,
			FirstEncodedValue: 7,
		},
		{
			Name:              "ArrayWithMultiNonMetricValue",
			Value:             bsonx.VC.ArrayFromValues(bsonx.VC.String("var"), bsonx.VC.String("bar")),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "ArrayWithMixedArrayFirstMetrics",
			Value:             bsonx.VC.ArrayFromValues(bsonx.VC.Boolean(true), bsonx.VC.String("var"), bsonx.VC.Int64(7)),
			NumEncodedValues:  2,
			ExpectedCount:     2,
			FirstEncodedValue: 1,
		},
		{
			Name:              "DoubleNoTruncate",
			Value:             bsonx.VC.Double(40.0),
			NumEncodedValues:  1,
			ExpectedCount:     1,
			FirstEncodedValue: 40,
		},
		{
			Name:              "DoubleTruncate",
			Value:             bsonx.VC.Double(40.20),
			NumEncodedValues:  1,
			ExpectedCount:     1,
			FirstEncodedValue: 40,
		},
		{
			Name:              "DateTime",
			Value:             bsonx.EC.Time("", now).Value(),
			ExpectedCount:     1,
			FirstEncodedValue: epochMs(now),
			NumEncodedValues:  1,
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
		Document           *bsonx.Document
		EncoderShouldError bool
		NumEncodedValues   int
		FirstEncodedValue  int64
	}{
		{
			Name:              "EmptyDocument",
			Document:          bsonx.NewDocument(),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "NilDocumentsDocument",
			Document:          (&bsonx.Document{IgnoreNilInsert: true}).Append(nil, nil),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "SingleMetricValue",
			Document:          bsonx.NewDocument(bsonx.EC.Int64("foo", 42)),
			NumEncodedValues:  1,
			FirstEncodedValue: 42,
		},
		{
			Name:              "MultiMetricValue",
			Document:          bsonx.NewDocument(bsonx.EC.Int64("foo", 7), bsonx.EC.Int32("foo", 72)),
			NumEncodedValues:  2,
			FirstEncodedValue: 7,
		},
		{
			Name:              "MultiNonMetricValue",
			Document:          bsonx.NewDocument(bsonx.EC.String("foo", "var"), bsonx.EC.String("bar", "bar")),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "MixedArrayFirstMetrics",
			Document:          bsonx.NewDocument(bsonx.EC.Boolean("zp", true), bsonx.EC.String("foo", "var"), bsonx.EC.Int64("bar", 7)),
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

func TestArrayExtraction(t *testing.T) {
	for _, test := range []struct {
		Name               string
		Array              *bsonx.Array
		EncoderShouldError bool
		NumEncodedValues   int
		FirstEncodedValue  int64
	}{
		{
			Name:              "EmptyArray",
			Array:             bsonx.NewArray(),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "SingleMetricValue",
			Array:             bsonx.NewArray(bsonx.VC.Int64(42)),
			NumEncodedValues:  1,
			FirstEncodedValue: 42,
		},
		{
			Name:              "MultiMetricValue",
			Array:             bsonx.NewArray(bsonx.VC.Int64(7), bsonx.VC.Int32(72)),
			NumEncodedValues:  2,
			FirstEncodedValue: 7,
		},
		{
			Name:              "MultiNonMetricValue",
			Array:             bsonx.NewArray(bsonx.VC.String("var"), bsonx.VC.String("bar")),
			NumEncodedValues:  0,
			FirstEncodedValue: 0,
		},
		{
			Name:              "MixedArrayFirstMetrics",
			Array:             bsonx.NewArray(bsonx.VC.Boolean(true), bsonx.VC.String("var"), bsonx.VC.Int64(7)),
			NumEncodedValues:  2,
			FirstEncodedValue: 1,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			metrics, err := extractMetricsFromArray(test.Array)
			assert.NoError(t, err)
			assert.Equal(t, test.NumEncodedValues, len(metrics))

			if test.NumEncodedValues >= 1 {
				assert.Equal(t, test.FirstEncodedValue, metrics[0])
			}
		})
	}
}

func TestMetricsHashValue(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name        string
		value       *bsonx.Value
		expectedNum int
		keyElems    int
	}{
		{
			name:        "IgnoredType",
			value:       bsonx.VC.Null(),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "ObjectID",
			value:       bsonx.VC.ObjectID(objectid.New()),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "String",
			value:       bsonx.VC.String("foo"),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "Decimal128",
			value:       bsonx.VC.Decimal128(decimal.NewDecimal128(42, 42)),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "BoolTrue",
			value:       bsonx.VC.Boolean(true),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "BoolFalse",
			value:       bsonx.VC.Boolean(false),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int32",
			value:       bsonx.VC.Int32(42),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int32Zero",
			value:       bsonx.VC.Int32(0),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int32Negative",
			value:       bsonx.VC.Int32(42),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int64",
			value:       bsonx.VC.Int64(42),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int64Zero",
			value:       bsonx.VC.Int64(0),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int64Negative",
			value:       bsonx.VC.Int64(42),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "DateTimeZero",
			value:       bsonx.VC.DateTime(0),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "DateTime",
			value:       bsonx.EC.Time("", now.Round(time.Second)).Value(),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "TimestampZero",
			value:       bsonx.VC.Timestamp(0, 0),
			expectedNum: 2,
			keyElems:    1,
		},
		{
			name:        "TimestampLarger",
			value:       bsonx.VC.Timestamp(42, 42),
			expectedNum: 2,
			keyElems:    1,
		},
		{
			name:        "EmptyDocument",
			value:       bsonx.EC.SubDocumentFromElements("data").Value(),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "SingleMetricValue",
			value:       bsonx.EC.SubDocumentFromElements("data", bsonx.EC.Int64("foo", 42)).Value(),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "MultiMetricValue",
			value:       bsonx.EC.SubDocumentFromElements("data", bsonx.EC.Int64("foo", 7), bsonx.EC.Int32("foo", 72)).Value(),
			expectedNum: 2,
			keyElems:    2,
		},
		{
			name:        "MultiNonMetricValue",
			value:       bsonx.EC.SubDocumentFromElements("data", bsonx.EC.String("foo", "var"), bsonx.EC.String("bar", "bar")).Value(),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "MixedArrayFirstMetrics",
			value:       bsonx.EC.SubDocumentFromElements("data", bsonx.EC.Boolean("zp", true), bsonx.EC.String("foo", "var"), bsonx.EC.Int64("bar", 7)).Value(),
			expectedNum: 2,
			keyElems:    2,
		},
		{
			name:        "ArraEmptyArray",
			value:       bsonx.VC.Array(bsonx.NewArray()),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "ArrayWithSingleMetricValue",
			value:       bsonx.VC.ArrayFromValues(bsonx.VC.Int64(42)),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "ArrayWithMultiMetricValue",
			value:       bsonx.VC.ArrayFromValues(bsonx.VC.Int64(7), bsonx.VC.Int32(72)),
			expectedNum: 2,
			keyElems:    2,
		},
		{
			name:        "ArrayWithMultiNonMetricValue",
			value:       bsonx.VC.ArrayFromValues(bsonx.VC.String("var"), bsonx.VC.String("bar")),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "ArrayWithMixedArrayFirstMetrics",
			value:       bsonx.VC.ArrayFromValues(bsonx.VC.Boolean(true), bsonx.VC.String("var"), bsonx.VC.Int64(7)),
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

func TestMetricsToElement(t *testing.T) {
	for _, test := range []struct {
		name       string
		ref        *bsonx.Element
		metrics    []Metric
		expected   *bsonx.Element
		outNum     int
		isDocument bool
	}{
		{
			name: "ObjectID",
			ref:  bsonx.EC.ObjectID("foo", objectid.New()),
		},
		{
			name: "String",
			ref:  bsonx.EC.String("foo", "bar"),
		},
		{
			name: "Regex",
			ref:  bsonx.EC.Regex("foo", "bar", "bar"),
		},
		{
			name: "Decimal128",
			ref:  bsonx.EC.Decimal128("foo", decimal.NewDecimal128(1, 2)),
		},
		{
			name: "Double",
			ref:  bsonx.EC.Double("foo", 4.42),
			metrics: []Metric{
				{Values: []int64{4}},
			},
			expected: bsonx.EC.Double("foo", 4.0),
			outNum:   1,
		},
		{
			name: "Short",
			ref:  bsonx.EC.Int32("foo", 4),
			metrics: []Metric{
				{Values: []int64{37}},
			},
			expected: bsonx.EC.Int32("foo", 37),
			outNum:   1,
		},
		{

			name: "FalseBool",
			ref:  bsonx.EC.Boolean("foo", true),
			metrics: []Metric{
				{Values: []int64{0}},
			},
			expected: bsonx.EC.Boolean("foo", false),
			outNum:   1,
		},
		{

			name: "TrueBool",
			ref:  bsonx.EC.Boolean("foo", false),
			metrics: []Metric{
				{Values: []int64{1}},
			},
			expected: bsonx.EC.Boolean("foo", true),
			outNum:   1,
		},
		{

			name: "SuperTrueBool",
			ref:  bsonx.EC.Boolean("foo", false),
			metrics: []Metric{
				{Values: []int64{100}},
			},
			expected: bsonx.EC.Boolean("foo", true),
			outNum:   1,
		},
		{

			name:       "EmptyDocument",
			ref:        bsonx.EC.SubDocument("foo", bsonx.NewDocument()),
			expected:   bsonx.EC.SubDocument("foo", bsonx.NewDocument()),
			isDocument: true,
		},
		{

			name: "DateTimeFromTime",
			ref:  bsonx.EC.Time("foo", time.Now()),
			metrics: []Metric{
				{Values: []int64{1000}},
			},
			expected: bsonx.EC.DateTime("foo", 1000),
			outNum:   1,
		},
		{

			name: "DateTime",
			ref:  bsonx.EC.DateTime("foo", 19999),
			metrics: []Metric{
				{Values: []int64{1000}},
			},
			expected: bsonx.EC.DateTime("foo", 1000),
			outNum:   1,
		},
		{

			name: "TimeStamp",
			ref:  bsonx.EC.Timestamp("foo", 19999, 100),
			metrics: []Metric{
				{Values: []int64{1000}},
				{Values: []int64{1000}},
			},
			expected: bsonx.EC.Timestamp("foo", 1000, 1000),
			outNum:   2,
		},
		{
			name:     "ArrayEmpty",
			ref:      bsonx.EC.ArrayFromElements("foo", bsonx.VC.String("foo"), bsonx.VC.String("bar")),
			expected: bsonx.EC.Array("foo", bsonx.NewArray()),
		},
		{
			name: "ArraySingle",
			metrics: []Metric{
				{Values: []int64{1}},
			},
			ref:      bsonx.EC.ArrayFromElements("foo", bsonx.VC.Boolean(true)),
			expected: bsonx.EC.Array("foo", bsonx.NewArray(bsonx.VC.Boolean(true))),
			outNum:   1,
		},
		{
			name: "ArrayMulti",
			metrics: []Metric{
				{Values: []int64{1}},
				{Values: []int64{77}},
			},
			ref:      bsonx.EC.ArrayFromElements("foo", bsonx.VC.Boolean(true), bsonx.VC.Int32(33)),
			expected: bsonx.EC.Array("foo", bsonx.NewArray(bsonx.VC.Boolean(true), bsonx.VC.Int32(77))),
			outNum:   2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			elem, num := rehydrateElement(test.ref, 0, test.metrics, 0)
			assert.Equal(t, test.outNum, num)
			if !test.isDocument {
				assert.Equal(t, test.expected, elem)
			} else {
				assert.True(t, test.expected.Value().MutableDocument().Equal(elem.Value().MutableDocument()))
			}

		})
	}
}

func TestIsOneChecker(t *testing.T) {
	assert.False(t, isNum(1, nil))
	assert.False(t, isNum(1, bsonx.VC.Int32(32)))
	assert.False(t, isNum(1, bsonx.VC.Int32(0)))
	assert.False(t, isNum(1, bsonx.VC.Int64(32)))
	assert.False(t, isNum(1, bsonx.VC.Int64(0)))
	assert.False(t, isNum(1, bsonx.VC.Double(32.2)))
	assert.False(t, isNum(1, bsonx.VC.Double(0.45)))
	assert.False(t, isNum(1, bsonx.VC.Double(0.0)))
	assert.False(t, isNum(1, bsonx.VC.String("foo")))
	assert.False(t, isNum(1, bsonx.VC.Boolean(true)))
	assert.False(t, isNum(1, bsonx.VC.Boolean(false)))

	assert.True(t, isNum(1, bsonx.VC.Int32(1)))
	assert.True(t, isNum(1, bsonx.VC.Int64(1)))
	assert.True(t, isNum(1, bsonx.VC.Double(1.0)))
}
