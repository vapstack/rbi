package rbi

import (
	"errors"
	"math"
	"math/rand/v2"
	"sort"
	"time"
)

const (
	// defaultAnalyzeSoftBudget bounds one periodic analyze cycle.
	// A zero/negative value means no time budget.
	defaultAnalyzeSoftBudget = 25 * time.Millisecond

	// plannerAnalyzeCardChunkSize controls how many index buckets are scanned
	// per short read-lock window.
	plannerAnalyzeCardChunkSize = 256
)

// RefreshPlannerStats rebuilds planner statistics from the current in-memory index.
// This is a synchronous, full refresh intended for explicit/manual calls.
func (db *DB[K, V]) RefreshPlannerStats() error {
	return db.refreshPlannerStatsWithBudget(0, false)
}

func (db *DB[K, V]) refreshPlannerStatsPeriodic() error {
	return db.refreshPlannerStatsWithBudget(db.planner.analyzer.softBudget, true)
}

func (db *DB[K, V]) refreshPlannerStatsWithBudget(softBudget time.Duration, useCursor bool) error {
	db.planner.analyzer.Lock()
	defer db.planner.analyzer.Unlock()

	fieldNames, universeCardinality, err := db.collectPlannerFieldNamesAndUniverse()
	if err != nil {
		return err
	}

	prev := db.planner.stats.Load()
	capHint := len(fieldNames)
	if prev != nil && len(prev.Fields) > capHint {
		capHint = len(prev.Fields)
	}

	out := PlannerStatsSnapshot{
		GeneratedAt:         time.Now(),
		UniverseCardinality: universeCardinality,
		Fields:              make(map[string]PlannerFieldStats, capHint),
	}

	// preserve previous field stats for fields not visited in this cycle
	if prev != nil {
		for k, v := range prev.Fields {
			out.Fields[k] = v
		}
	}

	startIdx := 0
	if useCursor && len(fieldNames) > 0 {
		if db.planner.analyzer.cursor < 0 || db.planner.analyzer.cursor >= len(fieldNames) {
			db.planner.analyzer.cursor = 0
		}
		startIdx = db.planner.analyzer.cursor
	}

	processed := 0
	deadline := time.Time{}
	if softBudget > 0 {
		deadline = time.Now().Add(softBudget)
	}

	for i := 0; i < len(fieldNames); i++ {
		if !deadline.IsZero() && processed > 0 && time.Now().After(deadline) {
			break
		}

		fieldIdx := i
		if useCursor {
			fieldIdx = (startIdx + i) % len(fieldNames)
		}
		fieldName := fieldNames[fieldIdx]

		stats, e := db.collectPlannerFieldStatsChunked(fieldName, plannerAnalyzeCardChunkSize)
		if e != nil {
			return e
		}
		out.Fields[fieldName] = stats
		processed++
	}

	if useCursor && len(fieldNames) > 0 && processed > 0 {
		db.planner.analyzer.cursor = (startIdx + processed) % len(fieldNames)
	} else if !useCursor {
		db.planner.analyzer.cursor = 0
	}

	out.Version = db.planner.statsVersion.Add(1)
	db.planner.stats.Store(&out)
	return nil
}

func (db *DB[K, V]) collectPlannerFieldNamesAndUniverse() ([]string, uint64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed.Load() {
		return nil, 0, ErrClosed
	}
	if db.noIndex.Load() {
		return nil, 0, ErrIndexDisabled
	}

	fieldNames := make([]string, 0, len(db.index))
	for fieldName := range db.index {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)

	return fieldNames, db.universe.GetCardinality(), nil
}

func (db *DB[K, V]) collectPlannerFieldStatsChunked(fieldName string, chunkSize int) (PlannerFieldStats, error) {
	if chunkSize <= 0 {
		chunkSize = plannerAnalyzeCardChunkSize
	}

	cards := make([]uint64, 0, 64)
	var (
		total    uint64
		maxCard  uint64
		nonEmpty uint64
		offset   int
	)

	for {
		db.mu.RLock()
		if db.closed.Load() {
			db.mu.RUnlock()
			return PlannerFieldStats{}, ErrClosed
		}
		if db.noIndex.Load() {
			db.mu.RUnlock()
			return PlannerFieldStats{}, ErrIndexDisabled
		}

		idxPtr := db.index[fieldName]
		if idxPtr == nil {
			db.mu.RUnlock()
			break
		}

		idx := *idxPtr
		if offset >= len(idx) {
			db.mu.RUnlock()
			break
		}

		end := offset + chunkSize
		if end > len(idx) {
			end = len(idx)
		}

		for i := offset; i < end; i++ {
			var card uint64
			if idx[i].IDs != nil {
				card = idx[i].IDs.GetCardinality()
			}
			cards = append(cards, card)
			total += card
			if card > maxCard {
				maxCard = card
			}
			if card > 0 {
				nonEmpty++
			}
		}
		offset = end
		db.mu.RUnlock()
	}

	if len(cards) == 0 {
		return PlannerFieldStats{}, nil
	}

	sort.Slice(cards, func(i, j int) bool {
		return cards[i] < cards[j]
	})

	return PlannerFieldStats{
		DistinctKeys:    uint64(len(cards)),
		NonEmptyKeys:    nonEmpty,
		TotalBucketCard: total,
		AvgBucketCard:   float64(total) / float64(len(cards)),
		MaxBucketCard:   maxCard,
		P50BucketCard:   percentileSortedU64(cards, 0.50),
		P95BucketCard:   percentileSortedU64(cards, 0.95),
	}, nil
}

const defaultPlannerAnalyzeInterval = time.Hour

func resolvePlannerAnalyzeInterval(v time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	if v == 0 {
		return defaultPlannerAnalyzeInterval
	}
	return v
}

func (db *DB[K, V]) startPlannerAnalyzeLoop() {
	interval := db.planner.analyzer.interval
	if interval <= 0 {
		return
	}
	if db.planner.analyzer.stop != nil {
		return
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	db.planner.analyzer.stop = stop
	db.planner.analyzer.done = done

	go db.runPlannerAnalyzeLoop(stop, done, interval)
}

func (db *DB[K, V]) stopAnalyzeLoop() {
	stop := db.planner.analyzer.stop
	done := db.planner.analyzer.done

	if stop != nil {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}
	if done != nil {
		<-done
	}

	db.planner.analyzer.stop = nil
	db.planner.analyzer.done = nil
}

func (db *DB[K, V]) runPlannerAnalyzeLoop(stop <-chan struct{}, done chan<- struct{}, base time.Duration) {
	defer close(done)

	rng := newRand(time.Now().UnixNano())
	failures := 0

	timer := time.NewTimer(nextAnalyzeDelay(base, failures, rng))
	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-timer.C:
		}

		err := db.refreshPlannerStatsPeriodic()
		if err != nil {
			if errors.Is(err, ErrClosed) {
				return
			}
			failures++
		} else {
			failures = 0
		}

		next := nextAnalyzeDelay(base, failures, rng)
		timer.Reset(next)
	}
}

func nextAnalyzeDelay(base time.Duration, failures int, rng *rand.Rand) time.Duration {
	d := base
	if failures > 0 {
		pow := failures
		if pow > 3 {
			pow = 3
		}
		d = base * time.Duration(1<<pow)
		m := base * 8
		if d > m {
			d = m
		}
	}
	return addPositiveJitter(d, rng)
}

func addPositiveJitter(d time.Duration, rng *rand.Rand) time.Duration {
	if d <= 0 {
		return d
	}
	// Add 0..20% positive jitter to de-synchronize multiple instances.
	j := d / 5
	if j <= 0 {
		return d
	}
	return d + time.Duration(rng.Int64N(int64(j)+1))
}

// PlannerStatsSnapshot is an immutable snapshot used by planner heuristics.
// The map and nested values are deep-copied on read to avoid caller mutation.
type PlannerStatsSnapshot struct {
	Version             uint64
	GeneratedAt         time.Time
	UniverseCardinality uint64
	Fields              map[string]PlannerFieldStats
}

type PlannerFieldStats struct {
	DistinctKeys    uint64
	NonEmptyKeys    uint64
	TotalBucketCard uint64
	AvgBucketCard   float64
	MaxBucketCard   uint64
	P50BucketCard   uint64
	P95BucketCard   uint64
}

func (db *DB[K, V]) refreshPlannerStatsLocked() {
	version := db.planner.statsVersion.Add(1)
	s := db.buildPlannerStatsSnapshotLocked(version)
	db.planner.stats.Store(s)
}

func (db *DB[K, V]) buildPlannerStatsSnapshotLocked(version uint64) *PlannerStatsSnapshot {
	out := &PlannerStatsSnapshot{
		Version:             version,
		GeneratedAt:         time.Now(),
		UniverseCardinality: db.universe.GetCardinality(),
		Fields:              make(map[string]PlannerFieldStats, len(db.index)),
	}

	for fieldName, idx := range db.index {
		if idx == nil {
			continue
		}

		buckets := *idx
		if len(buckets) == 0 {
			out.Fields[fieldName] = PlannerFieldStats{}
			continue
		}

		cards := make([]uint64, 0, len(buckets))
		var (
			total    uint64
			maxCard  uint64
			nonEmpty uint64
		)

		for _, b := range buckets {
			var card uint64
			if b.IDs != nil {
				card = b.IDs.GetCardinality()
			}
			cards = append(cards, card)
			total += card
			if card > maxCard {
				maxCard = card
			}
			if card > 0 {
				nonEmpty++
			}
		}

		sort.Slice(cards, func(i, j int) bool {
			return cards[i] < cards[j]
		})

		avg := 0.0
		if len(cards) > 0 {
			avg = float64(total) / float64(len(cards))
		}

		out.Fields[fieldName] = PlannerFieldStats{
			DistinctKeys:    uint64(len(cards)),
			NonEmptyKeys:    nonEmpty,
			TotalBucketCard: total,
			AvgBucketCard:   avg,
			MaxBucketCard:   maxCard,
			P50BucketCard:   percentileSortedU64(cards, 0.50),
			P95BucketCard:   percentileSortedU64(cards, 0.95),
		}
	}

	return out
}

func percentileSortedU64(sorted []uint64, q float64) uint64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[n-1]
	}

	// Nearest-rank percentile with 0-based index.
	pos := int(math.Ceil(q*float64(n))) - 1
	if pos < 0 {
		pos = 0
	}
	if pos >= n {
		pos = n - 1
	}
	return sorted[pos]
}

// GetPlannerStatsSnapshot returns a deep-copied planner stats snapshot.
func (db *DB[K, V]) GetPlannerStatsSnapshot() (PlannerStatsSnapshot, bool) {
	cur := db.planner.stats.Load()
	if cur == nil {
		return PlannerStatsSnapshot{}, false
	}
	return clonePlannerStatsSnapshot(*cur), true
}

func clonePlannerStatsSnapshot(s PlannerStatsSnapshot) PlannerStatsSnapshot {
	out := PlannerStatsSnapshot{
		Version:             s.Version,
		GeneratedAt:         s.GeneratedAt,
		UniverseCardinality: s.UniverseCardinality,
		Fields:              make(map[string]PlannerFieldStats, len(s.Fields)),
	}
	for k, v := range s.Fields {
		out.Fields[k] = v
	}
	return out
}
