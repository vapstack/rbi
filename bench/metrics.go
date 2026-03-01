package main

import (
	"math/bits"
	"sync/atomic"
	"time"
)

type intervalSnapshot struct {
	ReadP95Us  float64
	WriteP95Us float64
}

const intervalLatencyBuckets = 64

type latencyReservoir struct {
	values []float64
	cap    int
	seen   uint64
	state  uint64
}

func newLatencyReservoir(cap int, seed uint64) latencyReservoir {
	if cap < 1 {
		cap = 1
	}
	return latencyReservoir{
		values: make([]float64, 0, cap),
		cap:    cap,
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
	// splitmix64
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

type workerMetrics struct {
	opCounts map[string]uint64

	read  latencyReservoir
	write latencyReservoir

	writeQueue    latencyReservoir
	writeService  latencyReservoir
	writeEndToEnd latencyReservoir

	readIntervalHist  [intervalLatencyBuckets]atomic.Uint64
	writeIntervalHist [intervalLatencyBuckets]atomic.Uint64

	totalOps atomic.Uint64
	readOps  atomic.Uint64
	writeOps atomic.Uint64
	errors   atomic.Uint64
}

type RunMetrics struct {
	totalOps uint64
	readOps  uint64
	writeOps uint64
	errors   uint64

	workers []workerMetrics
}

func NewRunMetrics(maxSamples int, workers int) *RunMetrics {
	if maxSamples < 10_000 {
		maxSamples = 10_000
	}
	if workers < 1 {
		workers = 1
	}
	perWorkerCap := maxSamples / workers
	if perWorkerCap*workers < maxSamples {
		perWorkerCap++
	}
	if perWorkerCap < 256 {
		perWorkerCap = 256
	}

	ms := make([]workerMetrics, workers)
	for i := range ms {
		ms[i] = workerMetrics{
			opCounts:      make(map[string]uint64, 64),
			read:          newLatencyReservoir(perWorkerCap, uint64(i+1)*0x9e3779b97f4a7c15),
			write:         newLatencyReservoir(perWorkerCap, uint64(i+1)*0x94d049bb133111eb),
			writeQueue:    newLatencyReservoir(perWorkerCap, uint64(i+1)*0x6eed0e9da4d94a4f),
			writeService:  newLatencyReservoir(perWorkerCap, uint64(i+1)*0xd6e8feb86659fd93),
			writeEndToEnd: newLatencyReservoir(perWorkerCap, uint64(i+1)*0x94d049bb133111eb),
		}
	}
	return &RunMetrics{workers: ms}
}

func (m *RunMetrics) Record(workerID int, kind string, isWrite bool, duration time.Duration, err error) {
	atomic.AddUint64(&m.totalOps, 1)
	if isWrite {
		atomic.AddUint64(&m.writeOps, 1)
	} else {
		atomic.AddUint64(&m.readOps, 1)
	}
	if err != nil {
		atomic.AddUint64(&m.errors, 1)
	}

	ns := float64(duration.Nanoseconds())
	if kind == "" {
		kind = "unknown"
	}

	if workerID < 0 {
		workerID = 0
	}
	if n := len(m.workers); n > 0 {
		workerID %= n
		if workerID < 0 {
			workerID += n
		}
		w := &m.workers[workerID]
		w.totalOps.Add(1)
		if isWrite {
			w.writeOps.Add(1)
		} else {
			w.readOps.Add(1)
		}
		if err != nil {
			w.errors.Add(1)
		}
		w.opCounts[kind]++
		b := intervalLatencyBucket(duration)
		if isWrite {
			w.write.add(ns)
			w.writeIntervalHist[b].Add(1)
			w.writeQueue.add(0)
			w.writeService.add(ns)
			w.writeEndToEnd.add(ns)
		} else {
			w.read.add(ns)
			w.readIntervalHist[b].Add(1)
		}
	}
}

func (m *RunMetrics) RecordWriteDetailed(
	workerID int,
	kind string,
	queueWait time.Duration,
	service time.Duration,
	endToEnd time.Duration,
	err error,
) {
	atomic.AddUint64(&m.totalOps, 1)
	atomic.AddUint64(&m.writeOps, 1)
	if err != nil {
		atomic.AddUint64(&m.errors, 1)
	}

	if kind == "" {
		kind = "unknown"
	}
	if queueWait < 0 {
		queueWait = 0
	}
	if service < 0 {
		service = 0
	}
	if endToEnd < service {
		endToEnd = service
	}

	if workerID < 0 {
		workerID = 0
	}
	if n := len(m.workers); n > 0 {
		workerID %= n
		if workerID < 0 {
			workerID += n
		}
		w := &m.workers[workerID]
		w.totalOps.Add(1)
		w.writeOps.Add(1)
		if err != nil {
			w.errors.Add(1)
		}
		w.opCounts[kind]++
		w.write.add(float64(service.Nanoseconds()))
		w.writeIntervalHist[intervalLatencyBucket(service)].Add(1)
		w.writeQueue.add(float64(queueWait.Nanoseconds()))
		w.writeService.add(float64(service.Nanoseconds()))
		w.writeEndToEnd.add(float64(endToEnd.Nanoseconds()))
	}
}

func (m *RunMetrics) TakeIntervalSnapshot() intervalSnapshot {
	var readHist [intervalLatencyBuckets]uint64
	var writeHist [intervalLatencyBuckets]uint64

	for i := range m.workers {
		w := &m.workers[i]
		for b := 0; b < intervalLatencyBuckets; b++ {
			readHist[b] += w.readIntervalHist[b].Swap(0)
			writeHist[b] += w.writeIntervalHist[b].Swap(0)
		}
	}

	return intervalSnapshot{
		ReadP95Us:  intervalHistPercentileUs(readHist, 0.95),
		WriteP95Us: intervalHistPercentileUs(writeHist, 0.95),
	}
}

func (m *RunMetrics) SnapshotFinal() (read []float64, write []float64, opCounts map[string]uint64) {
	opCounts = make(map[string]uint64, 64)
	for i := range m.workers {
		w := &m.workers[i]
		read = append(read, w.read.values...)
		write = append(write, w.write.values...)
		for k, v := range w.opCounts {
			opCounts[k] += v
		}
	}
	return read, write, opCounts
}

func (m *RunMetrics) SnapshotFinalDetailed() (
	read []float64,
	write []float64,
	writeQueue []float64,
	writeService []float64,
	writeEndToEnd []float64,
	opCounts map[string]uint64,
) {
	opCounts = make(map[string]uint64, 64)
	for i := range m.workers {
		w := &m.workers[i]
		read = append(read, w.read.values...)
		write = append(write, w.write.values...)
		writeQueue = append(writeQueue, w.writeQueue.values...)
		writeService = append(writeService, w.writeService.values...)
		writeEndToEnd = append(writeEndToEnd, w.writeEndToEnd.values...)
		for k, v := range w.opCounts {
			opCounts[k] += v
		}
	}
	return read, write, writeQueue, writeService, writeEndToEnd, opCounts
}

func (m *RunMetrics) TotalOps() uint64 { return atomic.LoadUint64(&m.totalOps) }
func (m *RunMetrics) ReadOps() uint64  { return atomic.LoadUint64(&m.readOps) }
func (m *RunMetrics) WriteOps() uint64 { return atomic.LoadUint64(&m.writeOps) }
func (m *RunMetrics) Errors() uint64   { return atomic.LoadUint64(&m.errors) }

func intervalLatencyBucket(d time.Duration) int {
	ns := uint64(d.Nanoseconds())
	if ns == 0 {
		return 0
	}
	b := bits.Len64(ns) - 1
	if b < 0 {
		return 0
	}
	if b >= intervalLatencyBuckets {
		return intervalLatencyBuckets - 1
	}
	return b
}

func intervalBucketUpperNs(bucket int) uint64 {
	if bucket <= 0 {
		return 1
	}
	if bucket >= 63 {
		return ^uint64(0)
	}
	return uint64(1) << uint(bucket+1)
}

func intervalHistPercentileUs(hist [intervalLatencyBuckets]uint64, q float64) float64 {
	var total uint64
	for i := 0; i < intervalLatencyBuckets; i++ {
		total += hist[i]
	}
	if total == 0 {
		return 0
	}
	if q <= 0 {
		return float64(intervalBucketUpperNs(0)) / 1000.0
	}
	if q >= 1 {
		return float64(intervalBucketUpperNs(intervalLatencyBuckets-1)) / 1000.0
	}

	target := uint64(float64(total-1)*q) + 1
	var acc uint64
	for i := 0; i < intervalLatencyBuckets; i++ {
		acc += hist[i]
		if acc >= target {
			return float64(intervalBucketUpperNs(i)) / 1000.0
		}
	}
	return float64(intervalBucketUpperNs(intervalLatencyBuckets-1)) / 1000.0
}
