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
	defaultAnalyzeSoftBudget = 100 * time.Millisecond
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
	if err := db.beginOp(); err != nil {
		return err
	}
	defer db.endOp()

	db.planner.analyzer.Lock()
	defer db.planner.analyzer.Unlock()

	snap, fieldNames, universeCardinality, err := db.collectPlannerFieldNamesAndUniverse()
	if err != nil {
		return err
	}

	prev := db.planner.stats.Load()
	capHint := len(fieldNames)
	if prev != nil && len(prev.Fields) > capHint {
		capHint = len(prev.Fields)
	}

	out := plannerStatsSnapshot{
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

		stats, e := db.collectPlannerFieldStatsFromOverlay(snap, fieldName)
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

func (db *DB[K, V]) collectPlannerFieldNamesAndUniverse() (*indexSnapshot, []string, uint64, error) {
	if db.closed.Load() {
		return nil, nil, 0, ErrClosed
	}

	s := db.getSnapshot()
	fields := s.fieldNameSet()

	fieldNames := make([]string, 0, len(fields))
	for fieldName := range fields {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)

	return s, fieldNames, s.universeCardinality(), nil
}

func (s *indexSnapshot) universeCardinality() uint64 {
	base := uint64(0)
	if s.universe != nil {
		base = s.universe.GetCardinality()
	}
	if s.universeAdd != nil {
		base += s.universeAdd.GetCardinality()
	}
	if s.universeRem != nil {
		drop := s.universeRem.GetCardinality()
		if drop >= base {
			return 0
		}
		base -= drop
	}
	return base
}

func (db *DB[K, V]) collectPlannerFieldStatsFromOverlay(s *indexSnapshot, fieldName string) (PlannerFieldStats, error) {
	if db.closed.Load() {
		return PlannerFieldStats{}, ErrClosed
	}

	ov := newFieldOverlay(s.fieldIndexSlice(fieldName), s.fieldDelta(fieldName))
	if !ov.hasData() {
		return PlannerFieldStats{}, nil
	}
	return ov.fieldStats(), nil
}

func (ov fieldOverlay) fieldStats() PlannerFieldStats {
	cardBuf := getUint64SliceBuf(64)
	cards := cardBuf.values[:0]
	defer func() {
		cardBuf.values = cards
		releaseUint64SliceBuffer(cardBuf)
	}()
	var (
		total    uint64
		maxCard  uint64
		nonEmpty uint64
	)
	br := ov.rangeForBounds(rangeBounds{has: true})
	if br.baseStart >= br.baseEnd && br.deltaStart >= br.deltaEnd {
		return PlannerFieldStats{}
	}

	cur := ov.newCursor(br, false)
	for {
		_, baseIDs, de, ok := cur.next()
		if !ok {
			break
		}
		card := composePostingCardinality(baseIDs, de)
		cards = append(cards, card)
		total += card
		if card > maxCard {
			maxCard = card
		}
		if card > 0 {
			nonEmpty++
		}
	}

	if len(cards) == 0 {
		return PlannerFieldStats{}
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
	}
}

func plannerAnalyzeInterval(v time.Duration) time.Duration {
	if v < 0 {
		return 0
	}
	if v == 0 {
		return defaultOptionsAnalyzeInterval
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
			if errors.Is(err, ErrRebuildInProgress) {
				failures = 0
			} else {
				failures++
			}
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

// plannerStatsSnapshot is an immutable snapshot used by planner heuristics.
type plannerStatsSnapshot struct {
	Version             uint64
	GeneratedAt         time.Time
	UniverseCardinality uint64
	Fields              map[string]PlannerFieldStats
}

func (s *plannerStatsSnapshot) universeOr(fallback uint64) uint64 {
	if s != nil && s.UniverseCardinality > 0 {
		return s.UniverseCardinality
	}
	return fallback
}

// PlannerFieldStats contains per-field cardinality distribution metrics.
type PlannerFieldStats struct {
	// DistinctKeys is number of distinct keys in field index.
	DistinctKeys uint64
	// NonEmptyKeys is number of keys with non-empty posting bitmap.
	NonEmptyKeys uint64
	// TotalBucketCard is total cardinality summed across all field buckets.
	TotalBucketCard uint64
	// AvgBucketCard is average bucket cardinality.
	AvgBucketCard float64
	// MaxBucketCard is maximum bucket cardinality.
	MaxBucketCard uint64
	// P50BucketCard is median bucket cardinality.
	P50BucketCard uint64
	// P95BucketCard is 95th percentile bucket cardinality.
	P95BucketCard uint64
}

func (db *DB[K, V]) refreshPlannerStatsLocked() {
	version := db.planner.statsVersion.Add(1)
	s := db.buildPlannerStatsSnapshotLocked(version)
	db.planner.stats.Store(s)
}

func (db *DB[K, V]) buildPlannerStatsSnapshotLocked(version uint64) *plannerStatsSnapshot {
	snap := db.getSnapshot()
	fields := snap.fieldNameSet()

	out := &plannerStatsSnapshot{
		Version:             version,
		GeneratedAt:         time.Now(),
		UniverseCardinality: db.snapshotUniverseCardinality(),
		Fields:              make(map[string]PlannerFieldStats, len(fields)),
	}

	for fieldName := range fields {
		ov := newFieldOverlay(snap.fieldIndexSlice(fieldName), snap.fieldDelta(fieldName))
		out.Fields[fieldName] = ov.fieldStats()
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

	// nearest-rank percentile with 0-based index
	pos := int(math.Ceil(q*float64(n))) - 1
	if pos < 0 {
		pos = 0
	}
	if pos >= n {
		pos = n - 1
	}
	return sorted[pos]
}
