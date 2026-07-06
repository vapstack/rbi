package rbi

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
	"unsafe"
)

type codecBenchScalarRecord struct {
	ID      uint64
	Count   int64
	Age     int
	Score   float64
	Ratio   float32
	Active  bool
	Deleted bool
}

type codecBenchStringRecord struct {
	ID      uint64
	Name    string
	Email   string
	Country string
	Plan    string
	Status  string
	Role    string
	Team    string
	Region  string
	Comment string
}

type codecBenchNestedRecord struct {
	ID      uint64
	Name    string
	Profile codecBenchProfile
	Metrics codecBenchMetrics
	Window  codecBenchWindow
}

type codecBenchPointerRecord struct {
	ID       uint64
	Name     *string
	Age      *int
	Score    *float64
	Active   *bool
	Window   *codecBenchWindow
	NilValue *string
}

type codecBenchProfile struct {
	Country string
	Plan    string
	Age     int
	Score   float64
}

type codecBenchMetrics struct {
	Seen      uint64
	Clicked   uint64
	Converted uint64
	Revenue   float64
}

type codecBenchWindow struct {
	From time.Time
	To   time.Time
}

type codecBenchSliceRecord struct {
	ID      uint64
	Name    string
	Tags    []string
	Scores  []int64
	Ratios  []float64
	Windows []codecBenchWindow
}

type codecBenchMapRecord struct {
	ID     uint64
	Name   string
	Labels map[string]string
	Counts map[string]int64
	Scores map[uint64]float64
	Nested map[string]codecBenchMapValue
}

type codecBenchMapValue struct {
	Count int64
	Tag   string
}

var (
	codecBenchByteSink uint64
)

func openCodecBenchCollection[V any](b *testing.B) *Collection[uint64, V] {
	b.Helper()
	path := filepath.Join(b.TempDir(), "codec-benchmark.db")
	c, bolt := openBoltAndCollection[uint64, V](b, path, Options{
		DisableIndexLoad:  true,
		DisableIndexStore: true,
	})
	b.Cleanup(func() {
		_ = c.Close()
		_ = bolt.Close()
	})
	return c
}

func codecBenchScalarValue() *codecBenchScalarRecord {
	return &codecBenchScalarRecord{
		ID:      123456789,
		Count:   -987654321,
		Age:     42,
		Score:   12345.875,
		Ratio:   0.625,
		Active:  true,
		Deleted: false,
	}
}

func codecBenchStringValue() *codecBenchStringRecord {
	return &codecBenchStringRecord{
		ID:      123456789,
		Name:    "alice-example-long-name",
		Email:   "alice@example.internal",
		Country: "NL",
		Plan:    "enterprise",
		Status:  "active",
		Role:    "admin",
		Team:    "storage",
		Region:  "eu-west",
		Comment: "record payload with enough string data to exercise decoder allocation cost",
	}
}

func codecBenchNestedValue() *codecBenchNestedRecord {
	return &codecBenchNestedRecord{
		ID:   123456789,
		Name: "nested-alice",
		Profile: codecBenchProfile{
			Country: "US",
			Plan:    "pro",
			Age:     37,
			Score:   984.25,
		},
		Metrics: codecBenchMetrics{
			Seen:      5000,
			Clicked:   730,
			Converted: 41,
			Revenue:   1234.5,
		},
		Window: codecBenchWindow{
			From: time.Unix(1_700_000_000, 123).UTC(),
			To:   time.Unix(1_700_086_400, 456).UTC(),
		},
	}
}

func codecBenchPointerValue() *codecBenchPointerRecord {
	name := "pointer-alice"
	age := 41
	score := 990.5
	active := true
	window := codecBenchWindow{
		From: time.Unix(1_700_000_000, 123).UTC(),
		To:   time.Unix(1_700_000_500, 456).UTC(),
	}
	return &codecBenchPointerRecord{
		ID:     123456789,
		Name:   &name,
		Age:    &age,
		Score:  &score,
		Active: &active,
		Window: &window,
	}
}

func codecBenchSliceValue() *codecBenchSliceRecord {
	return &codecBenchSliceRecord{
		ID:     123456789,
		Name:   "slice-alice",
		Tags:   []string{"go", "db", "storage", "query", "codec", "benchmark"},
		Scores: []int64{1, 2, 3, 5, 8, 13, 21, 34},
		Ratios: []float64{0.1, 0.2, 0.3, 0.5, 0.8},
		Windows: []codecBenchWindow{
			{From: time.Unix(1_700_000_000, 0).UTC(), To: time.Unix(1_700_000_100, 0).UTC()},
			{From: time.Unix(1_700_000_200, 0).UTC(), To: time.Unix(1_700_000_300, 0).UTC()},
			{From: time.Unix(1_700_000_400, 0).UTC(), To: time.Unix(1_700_000_500, 0).UTC()},
		},
	}
}

func codecBenchMapValueRecord() *codecBenchMapRecord {
	return &codecBenchMapRecord{
		ID:   123456789,
		Name: "map-alice",
		Labels: map[string]string{
			"country": "US",
			"plan":    "enterprise",
			"status":  "active",
			"region":  "us-east",
			"team":    "storage",
		},
		Counts: map[string]int64{
			"seen":      5000,
			"clicked":   730,
			"converted": 41,
			"failed":    3,
		},
		Scores: map[uint64]float64{
			10: 1.25,
			20: 2.5,
			30: 3.75,
			40: 5.0,
		},
		Nested: map[string]codecBenchMapValue{
			"alpha": {Count: 1, Tag: "a"},
			"beta":  {Count: 2, Tag: "b"},
			"gamma": {Count: 3, Tag: "c"},
		},
	}
}

func codecBenchCollectionPayload[V any](b *testing.B, c *Collection[uint64, V], rec *V) []byte {
	b.Helper()
	var buf bytes.Buffer
	c.encode(rec, &buf)
	payload := append([]byte(nil), buf.Bytes()...)
	decoded, err := c.decode(payload)
	if err != nil {
		b.Fatalf("decode warmup: %v", err)
	}
	c.ReleaseRecords(decoded)
	return payload
}

func benchmarkCodecCollectionEncode[V any](b *testing.B, c *Collection[uint64, V], rec *V) {
	payload := codecBenchCollectionPayload(b, c, rec)
	var buf bytes.Buffer
	buf.Grow(len(payload))
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	var written uint64
	for b.Loop() {
		buf.Reset()
		c.encode(rec, &buf)
		written += uint64(buf.Len())
	}
	b.ReportMetric(float64(len(payload)), "payload_B/op")
	codecBenchByteSink = written
}

func benchmarkCodecCollectionDecode[V any](b *testing.B, c *Collection[uint64, V], rec *V, consume func(*V) uint64) {
	payload := codecBenchCollectionPayload(b, c, rec)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	var consumed uint64
	for b.Loop() {
		decoded, err := c.decode(payload)
		if err != nil {
			b.Fatalf("decode: %v", err)
		}
		consumed += consume(decoded)
		c.ReleaseRecords(decoded)
	}
	b.ReportMetric(float64(len(payload)), "payload_B/op")
	codecBenchByteSink = consumed
}

func Benchmark_Codec_Collection_Encode(b *testing.B) {
	b.Run("Scalar", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchScalarRecord](b)
		benchmarkCodecCollectionEncode(b, c, codecBenchScalarValue())
	})
	b.Run("StringHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchStringRecord](b)
		benchmarkCodecCollectionEncode(b, c, codecBenchStringValue())
	})
	b.Run("Nested", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchNestedRecord](b)
		benchmarkCodecCollectionEncode(b, c, codecBenchNestedValue())
	})
	b.Run("PointerHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchPointerRecord](b)
		benchmarkCodecCollectionEncode(b, c, codecBenchPointerValue())
	})
	b.Run("SliceHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchSliceRecord](b)
		benchmarkCodecCollectionEncode(b, c, codecBenchSliceValue())
	})
	b.Run("MapHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchMapRecord](b)
		benchmarkCodecCollectionEncode(b, c, codecBenchMapValueRecord())
	})
}

func Benchmark_Codec_Collection_Decode(b *testing.B) {
	b.Run("Scalar", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchScalarRecord](b)
		benchmarkCodecCollectionDecode(b, c, codecBenchScalarValue(), func(v *codecBenchScalarRecord) uint64 {
			out := v.ID + uint64(v.Age) + uint64(v.Count)
			if v.Active {
				out++
			}
			if v.Deleted {
				out++
			}
			return out
		})
	})
	b.Run("StringHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchStringRecord](b)
		benchmarkCodecCollectionDecode(b, c, codecBenchStringValue(), func(v *codecBenchStringRecord) uint64 {
			return uint64(v.ID) + uint64(len(v.Name)+len(v.Email)+len(v.Comment))
		})
	})
	b.Run("Nested", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchNestedRecord](b)
		benchmarkCodecCollectionDecode(b, c, codecBenchNestedValue(), func(v *codecBenchNestedRecord) uint64 {
			return v.ID + v.Metrics.Seen + uint64(v.Profile.Age) + uint64(v.Window.To.Sub(v.Window.From))
		})
	})
	b.Run("PointerHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchPointerRecord](b)
		benchmarkCodecCollectionDecode(b, c, codecBenchPointerValue(), func(v *codecBenchPointerRecord) uint64 {
			out := v.ID
			if v.Name != nil {
				out += uint64(len(*v.Name))
			}
			if v.Age != nil {
				out += uint64(*v.Age)
			}
			if v.Window != nil {
				out += uint64(v.Window.To.Sub(v.Window.From))
			}
			return out
		})
	})
	b.Run("SliceHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchSliceRecord](b)
		benchmarkCodecCollectionDecode(b, c, codecBenchSliceValue(), func(v *codecBenchSliceRecord) uint64 {
			return v.ID + uint64(len(v.Tags)+len(v.Scores)+len(v.Ratios)+len(v.Windows))
		})
	})
	b.Run("MapHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchMapRecord](b)
		benchmarkCodecCollectionDecode(b, c, codecBenchMapValueRecord(), func(v *codecBenchMapRecord) uint64 {
			return v.ID + uint64(len(v.Labels)+len(v.Counts)+len(v.Scores)+len(v.Nested))
		})
	})
}

func codecBenchDirectPayload[V any](b *testing.B, c *Collection[uint64, V], rec *V) []byte {
	b.Helper()
	var buf bytes.Buffer
	c.schema.Codec.Encode(unsafe.Pointer(rec), &buf)
	payload := append([]byte(nil), buf.Bytes()...)
	var dst V
	if err := c.schema.Codec.Decode(payload, unsafe.Pointer(&dst)); err != nil {
		b.Fatalf("direct decode warmup: %v", err)
	}
	return payload
}

func benchmarkCodecDirectEncode[V any](b *testing.B, c *Collection[uint64, V], rec *V) {
	payload := codecBenchDirectPayload(b, c, rec)
	var buf bytes.Buffer
	buf.Grow(len(payload))
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	var written uint64
	for b.Loop() {
		buf.Reset()
		c.schema.Codec.Encode(unsafe.Pointer(rec), &buf)
		written += uint64(buf.Len())
	}
	b.ReportMetric(float64(len(payload)), "payload_B/op")
	codecBenchByteSink = written
}

func benchmarkCodecDirectDecode[V any](b *testing.B, c *Collection[uint64, V], rec *V, consume func(*V) uint64) {
	payload := codecBenchDirectPayload(b, c, rec)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	var consumed uint64
	for b.Loop() {
		dst := c.recPool.Get()
		if err := c.schema.Codec.Decode(payload, unsafe.Pointer(dst)); err != nil {
			b.Fatalf("direct decode: %v", err)
		}
		consumed += consume(dst)
		c.ReleaseRecords(dst)
	}
	b.ReportMetric(float64(len(payload)), "payload_B/op")
	codecBenchByteSink = consumed
}

func Benchmark_Codec_Direct_Encode(b *testing.B) {
	b.Run("Scalar", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchScalarRecord](b)
		benchmarkCodecDirectEncode(b, c, codecBenchScalarValue())
	})
	b.Run("StringHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchStringRecord](b)
		benchmarkCodecDirectEncode(b, c, codecBenchStringValue())
	})
	b.Run("Nested", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchNestedRecord](b)
		benchmarkCodecDirectEncode(b, c, codecBenchNestedValue())
	})
	b.Run("PointerHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchPointerRecord](b)
		benchmarkCodecDirectEncode(b, c, codecBenchPointerValue())
	})
	b.Run("SliceHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchSliceRecord](b)
		benchmarkCodecDirectEncode(b, c, codecBenchSliceValue())
	})
	b.Run("MapHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchMapRecord](b)
		benchmarkCodecDirectEncode(b, c, codecBenchMapValueRecord())
	})
}

func Benchmark_Codec_Direct_Decode(b *testing.B) {
	b.Run("Scalar", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchScalarRecord](b)
		benchmarkCodecDirectDecode(b, c, codecBenchScalarValue(), func(v *codecBenchScalarRecord) uint64 {
			out := v.ID + uint64(v.Age) + uint64(v.Count)
			if v.Active {
				out++
			}
			if v.Deleted {
				out++
			}
			return out
		})
	})
	b.Run("StringHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchStringRecord](b)
		benchmarkCodecDirectDecode(b, c, codecBenchStringValue(), func(v *codecBenchStringRecord) uint64 {
			return uint64(v.ID) + uint64(len(v.Name)+len(v.Email)+len(v.Comment))
		})
	})
	b.Run("Nested", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchNestedRecord](b)
		benchmarkCodecDirectDecode(b, c, codecBenchNestedValue(), func(v *codecBenchNestedRecord) uint64 {
			return v.ID + v.Metrics.Seen + uint64(v.Profile.Age) + uint64(v.Window.To.Sub(v.Window.From))
		})
	})
	b.Run("PointerHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchPointerRecord](b)
		benchmarkCodecDirectDecode(b, c, codecBenchPointerValue(), func(v *codecBenchPointerRecord) uint64 {
			out := v.ID
			if v.Name != nil {
				out += uint64(len(*v.Name))
			}
			if v.Age != nil {
				out += uint64(*v.Age)
			}
			if v.Window != nil {
				out += uint64(v.Window.To.Sub(v.Window.From))
			}
			return out
		})
	})
	b.Run("SliceHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchSliceRecord](b)
		benchmarkCodecDirectDecode(b, c, codecBenchSliceValue(), func(v *codecBenchSliceRecord) uint64 {
			return v.ID + uint64(len(v.Tags)+len(v.Scores)+len(v.Ratios)+len(v.Windows))
		})
	})
	b.Run("MapHeavy", func(b *testing.B) {
		c := openCodecBenchCollection[codecBenchMapRecord](b)
		benchmarkCodecDirectDecode(b, c, codecBenchMapValueRecord(), func(v *codecBenchMapRecord) uint64 {
			return v.ID + uint64(len(v.Labels)+len(v.Counts)+len(v.Scores)+len(v.Nested))
		})
	})
}
