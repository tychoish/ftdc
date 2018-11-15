package ftdc

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/bsoncore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectorInterface(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Parallel()
	collectors := createCollectors()
	for _, collect := range collectors {
		t.Run(collect.name, func(t *testing.T) {
			tests := createTests()

			for _, test := range tests {
				if testing.Short() {
					continue
				}

				t.Run(test.name, func(t *testing.T) {
					collector := collect.factory()

					assert.NotPanics(t, func() {
						collector.SetMetadata(createEventRecord(42, int64(time.Minute), rand.Int63n(7), 4))
					})

					info := collector.Info()
					assert.Zero(t, info)

					for _, d := range test.docs {
						assert.NoError(t, collector.Add(d))
					}
					info = collector.Info()

					if test.randStats {
						assert.True(t, info.MetricsCount >= test.numStats,
							"%d >= %d", info.MetricsCount, test.numStats)
					} else {
						if !assert.Equal(t, test.numStats, info.MetricsCount) {
							fmt.Println(test.docs)
							fmt.Println(info)
						}
					}

					out, err := collector.Resolve()
					if len(test.docs) > 0 {
						assert.NoError(t, err)
						assert.NotZero(t, out)
					} else {
						assert.Error(t, err)
						assert.Zero(t, out)
					}

					collector.Reset()
					info = collector.Info()
					assert.Zero(t, info)
				})
			}
			t.Run("ResolveWhenNil", func(t *testing.T) {
				collector := collect.factory()
				out, err := collector.Resolve()
				assert.Nil(t, out)
				assert.Error(t, err)
			})
			t.Run("RoundTrip", func(t *testing.T) {
				for name, docs := range map[string][]bson.Raw{
					"Integers": []bson.Raw{
						randFlatDocument(5),
						randFlatDocument(5),
						randFlatDocument(5),
						randFlatDocument(5),
					},
					"DecendingHandIntegers": []bson.Raw{
						bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element(bsoncore.AppendInt64Element([]byte{}, "two", 5), "one", 43)),
						bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element(bsoncore.AppendInt64Element([]byte{}, "two", 4), "one", 89)),
						bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element(bsoncore.AppendInt64Element([]byte{}, "two", 3), "one", 99)),
						bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element(bsoncore.AppendInt64Element([]byte{}, "two", 2), "one", 101)),
					},
				} {
					t.Run(name, func(t *testing.T) {
						collector := collect.factory()
						count := 0
						for _, d := range docs {
							count++
							assert.NoError(t, collector.Add(d))
						}
						info := collector.Info()
						require.Equal(t, info.SampleCount, count)

						out, err := collector.Resolve()
						require.NoError(t, err)
						buf := bytes.NewBuffer(out)

						iter := ReadStructuredMetrics(ctx, buf)
						idx := -1
						for iter.Next() {
							idx++
							t.Run(fmt.Sprintf("DocumentNumber_%d", idx), func(t *testing.T) {
								s := iter.Document()

								if !assert.Equal(t, s, docs[idx]) {
									fmt.Println("---", idx)
									fmt.Println("in: ", docs[idx])
									fmt.Println("out:", s)
								}
							})
						}
						assert.Equal(t, len(docs)-1, idx) // zero index
						require.NoError(t, iter.Err())

					})
				}
			})
		})
	}
}

func TestStreamingEncoding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, impl := range []struct {
		name    string
		factory func() (Collector, *bytes.Buffer)
	}{
		{
			name: "StreamingDynamic",
			factory: func() (Collector, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				return NewStreamingDynamicCollector(100, buf), buf
			},
		},
		{
			name: "StreamingDynamicSmall",
			factory: func() (Collector, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				return NewStreamingDynamicCollector(2, buf), buf
			},
		},
	} {
		t.Run(impl.name, func(t *testing.T) {
			for _, test := range createEncodingTests() {
				t.Run(test.name, func(t *testing.T) {
					t.Run("SingleValues", func(t *testing.T) {
						collector, buf := impl.factory()
						for _, val := range test.dataset {
							assert.NoError(t, collector.Add(bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element([]byte{}, "foo", val))))
						}
						require.NoError(t, FlushCollector(collector, buf))
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						idx := 0
						for iter.Next() {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							assert.Equal(t, val, test.dataset[idx])
							idx++
						}
						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						if !assert.Equal(t, test.dataset, res) {
							grip.Infoln("in:", test.dataset)
							grip.Infoln("out:", res)
						}
					})
					t.Run("MultipleValues", func(t *testing.T) {
						collector, buf := impl.factory()
						docs := []bson.Raw{}

						for _, val := range test.dataset {
							doc := testEncodingDocFromValue(val)
							docs = append(docs, doc)
							assert.NoError(t, collector.Add(doc))
						}

						require.NoError(t, FlushCollector(collector, buf))
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next() {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							assert.Equal(t, doc, docs[idx])
						}

						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						assert.Equal(t, test.dataset, res)
					})

					t.Run("MultiValueKeyOrder", func(t *testing.T) {
						collector, buf := impl.factory()
						docs := []bson.Raw{}

						for idx, val := range test.dataset {
							var doc bson.Raw
							if len(test.dataset) >= 3 && (idx == 2 || idx == 3) {
								doc = testShortEncodingDocFromValue(val)
							} else {
								doc = testEncodingDocFromValue(val)
							}

							docs = append(docs, doc)
							assert.NoError(t, collector.Add(doc))
						}
						require.NoError(t, FlushCollector(collector, buf))
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next() {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							assert.Equal(t, doc, docs[idx])
						}

						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res), "%v -> %v", test.dataset, res)
						assert.Equal(t, test.dataset, res)
					})
					t.Run("DifferentKeys", func(t *testing.T) {
						collector, buf := impl.factory()
						docs := []bson.Raw{}

						for idx, val := range test.dataset {
							var doc bson.Raw
							if len(test.dataset) >= 5 && (idx == 2 || idx == 3) {
								doc = testEncodingDocAlternateFromValue(val)
							} else {
								doc = testEncodingDocFromValue(val)
							}

							docs = append(docs, doc)
							assert.NoError(t, collector.Add(doc))
						}

						require.NoError(t, FlushCollector(collector, buf))
						payload := buf.Bytes()

						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next() {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1
							assert.Equal(t, doc, docs[idx])
						}
						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res), "%v -> %v", test.dataset, res)
						require.Equal(t, len(test.dataset), len(res))
					})
				})
			}
		})
	}
}

func TestFixedEncoding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	for _, impl := range []struct {
		name    string
		factory func() Collector
	}{
		{
			name:    "Better",
			factory: func() Collector { return &betterCollector{maxDeltas: 20} },
		},
		{
			name:    "StableDynamic",
			factory: func() Collector { return NewDynamicCollector(100) },
		},
		{
			name:    "Streaming",
			factory: func() Collector { return newStreamingCollector(20, &bytes.Buffer{}) },
		},
	} {
		t.Run(impl.name, func(t *testing.T) {
			for _, test := range createEncodingTests() {
				t.Run(test.name, func(t *testing.T) {
					t.Run("SingleValues", func(t *testing.T) {
						collector := impl.factory()
						for _, val := range test.dataset {
							assert.NoError(t, collector.Add(bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element([]byte{}, "foo", val))))
						}

						payload, err := collector.Resolve()
						require.NoError(t, err)
						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						idx := 0
						for iter.Next() {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							assert.Equal(t, val, test.dataset[idx])
							idx++
						}
						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						if !assert.Equal(t, test.dataset, res) {
							grip.Infoln("in:", test.dataset)
							grip.Infoln("out:", res)
						}
					})
					t.Run("MultipleValues", func(t *testing.T) {
						collector := impl.factory()
						docs := []bson.Raw{}

						for _, val := range test.dataset {
							doc := testEncodingDocFromValue(val)
							docs = append(docs, doc)
							assert.NoError(t, collector.Add(doc))
						}

						payload, err := collector.Resolve()
						require.NoError(t, err)
						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next() {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							assert.Equal(t, doc, docs[idx])
						}

						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						assert.Equal(t, test.dataset, res)
					})
				})
			}
			t.Run("SizeMismatch", func(t *testing.T) {
				collector := impl.factory()

				assert.NoError(t, collector.Add(mockTwoElementDocument()))
				assert.NoError(t, collector.Add(mockTwoElementDocument()))

				if strings.Contains(impl.name, "Dynamic") {
					assert.NoError(t, collector.Add(bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element([]byte{}, "one", 43))))
				} else {
					assert.Error(t, collector.Add(bsoncore.BuildDocument([]byte{}, bsoncore.AppendInt64Element([]byte{}, "one", 43))))
				}
			})
		})
	}
}

func TestCollectorSizeCap(t *testing.T) {
	for _, test := range []struct {
		name    string
		factory func() Collector
	}{
		{
			name:    "Better",
			factory: func() Collector { return &betterCollector{maxDeltas: 1} },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			collector := test.factory()
			assert.NoError(t, collector.Add(mockTwoElementDocument()))
			assert.NoError(t, collector.Add(mockTwoElementDocument()))
			assert.NoError(t, collector.Add(mockTwoElementDocument()))
		})
	}
}

func TestWriter(t *testing.T) {
	t.Run("NilDocuments", func(t *testing.T) {
		collector := NewWriterCollector(2, &noopWriter{})
		_, err := collector.Write(nil)
		assert.Error(t, err)
		assert.NoError(t, collector.Close())
	})
	t.Run("RealDocument", func(t *testing.T) {
		collector := NewWriterCollector(2, &noopWriter{})
		_, err := collector.Write(mockTwoElementDocument())
		assert.NoError(t, err)
		assert.NoError(t, collector.Close())
	})
	t.Run("CloseNoError", func(t *testing.T) {
		collector := NewWriterCollector(2, &noopWriter{})
		assert.NoError(t, collector.Close())
	})
	t.Run("CloseError", func(t *testing.T) {
		collector := NewWriterCollector(2, &errWriter{})
		_, err := collector.Write(mockTwoElementDocument())
		require.NoError(t, err)
		assert.Error(t, collector.Close())
	})
}
