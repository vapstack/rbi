package main

import (
	"sort"
	"sync"
	"sync/atomic"
)

const maxLatencySampleSize = MaxLatencySampleSize

type latencyReservoir struct {
	values []float64
	cap    int
	seen   uint64
	state  uint64
}

type queryMetrics struct {
	count  atomic.Uint64
	errors atomic.Uint64
	lat    latencyReservoir
}

type workerMetrics struct {
	mu sync.Mutex

	total     atomic.Uint64
	errors    atomic.Uint64
	jitterNS  atomic.Uint64
	lastQuery atomic.Value

	totalLat          latencyReservoir
	trackQueries      bool
	trackQueryLatency bool
	queries           map[string]*queryMetrics
	queryCap          int
}

func newLatencyReservoir(capacity int, seed uint64) latencyReservoir {
	if capacity < 1 {
		capacity = 1
	}
	return latencyReservoir{
		values: make([]float64, 0, capacity),
		cap:    capacity,
		state:  seed,
	}
}

func (r *latencyReservoir) add(v float64) {
	r.seen++
	if len(r.values) < r.cap {
		r.values = append(r.values, v)
		return
	}
	j := r.next() % r.seen
	if j < uint64(r.cap) {
		r.values[int(j)] = v
	}
}

func (r *latencyReservoir) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func newWorkerMetrics(totalCap, queryCap int, seed uint64, trackQueries, trackQueryLatency bool) *workerMetrics {
	if totalCap < 32 {
		totalCap = 32
	}
	if queryCap < 16 {
		queryCap = 16
	}
	out := &workerMetrics{
		totalLat:          newLatencyReservoir(totalCap, seed),
		trackQueries:      trackQueries,
		trackQueryLatency: trackQueryLatency,
		queryCap:          queryCap,
	}
	if trackQueries {
		out.queries = make(map[string]*queryMetrics, 8)
	}
	return out
}

func (m *workerMetrics) observe(kind string, latencyNS float64, errCount uint64) {
	if kind == "" {
		kind = "unknown"
	}
	m.total.Add(1)
	if errCount > 0 {
		m.errors.Add(errCount)
	}
	m.lastQuery.Store(kind)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalLat.add(latencyNS)
	if !m.trackQueries {
		return
	}
	query := m.queries[kind]
	if query == nil {
		query = &queryMetrics{}
		if m.trackQueryLatency {
			query.lat = newLatencyReservoir(m.queryCap, uint64(len(m.queries)+1)*0x94d049bb133111eb)
		}
		m.queries[kind] = query
	}
	if m.trackQueryLatency {
		query.lat.add(latencyNS)
	}
	query.count.Add(1)
	if errCount > 0 {
		query.errors.Add(errCount)
	}
}

func (m *workerMetrics) counts() (uint64, uint64) {
	return m.total.Load(), m.errors.Load()
}

func (m *workerMetrics) addJitter(ns uint64) {
	if ns == 0 {
		return
	}
	m.jitterNS.Add(ns)
}

type workerMetricsSnapshot struct {
	total     uint64
	errors    uint64
	jitterNS  uint64
	lastQuery string
	latency   []float64
	queries   map[string]queryMetricsSnapshot
}

type queryMetricsSnapshot struct {
	count   uint64
	errors  uint64
	latency []float64
}

func (m *workerMetrics) snapshot(includeLatency bool) workerMetricsSnapshot {
	out := workerMetricsSnapshot{
		total:    m.total.Load(),
		errors:   m.errors.Load(),
		jitterNS: m.jitterNS.Load(),
	}
	if value := m.lastQuery.Load(); value != nil {
		if s, ok := value.(string); ok {
			out.lastQuery = s
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if includeLatency {
		out.latency = append([]float64(nil), m.totalLat.values...)
	}
	if len(m.queries) == 0 {
		return out
	}
	out.queries = make(map[string]queryMetricsSnapshot, len(m.queries))
	for name, query := range m.queries {
		querySnap := queryMetricsSnapshot{
			count:  query.count.Load(),
			errors: query.errors.Load(),
		}
		if includeLatency && m.trackQueryLatency {
			querySnap.latency = append([]float64(nil), query.lat.values...)
		}
		out.queries[name] = querySnap
	}
	return out
}

func perWorkerLatencyCap(workerCount int) int {
	if workerCount < 1 {
		workerCount = 1
	}
	capacity := maxLatencySampleSize / workerCount
	if capacity*workerCount < maxLatencySampleSize {
		capacity++
	}
	if capacity < 96 {
		capacity = 96
	}
	return capacity
}

func summarizeLatencies(data []float64) LatencySummary {
	if len(data) == 0 {
		return LatencySummary{}
	}
	sort.Float64s(data)
	var sum float64
	for _, v := range data {
		sum += v
	}
	const nsPerUs = 1000.0
	return LatencySummary{
		AvgUs: (sum / float64(len(data))) / nsPerUs,
		P50Us: percentileSorted(data, 0.50) / nsPerUs,
		P95Us: percentileSorted(data, 0.95) / nsPerUs,
		P99Us: percentileSorted(data, 0.99) / nsPerUs,
	}
}

func percentileSorted(data []float64, q float64) float64 {
	if len(data) == 0 {
		return 0
	}
	if q <= 0 {
		return data[0]
	}
	if q >= 1 {
		return data[len(data)-1]
	}
	idx := int(float64(len(data)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(data) {
		idx = len(data) - 1
	}
	return data[idx]
}
