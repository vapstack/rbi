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
//
// This is a synchronous, full refresh intended for explicit/manual calls.
//
// In indexed mode it scans the current published index snapshot and replaces
// the planner stats payload atomically.
//
// In transparent mode planner stats are disabled because no runtime index
// exists; the method returns ErrNoIndex.
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

	if db.transparent {
		return ErrNoIndex
	}

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
	if err := db.unavailableErr(); err != nil {
		return nil, nil, 0, err
	}

	s := db.getSnapshot()
	fieldNames := make([]string, 0, len(db.indexedFieldAccess))
	for _, acc := range db.indexedFieldAccess {
		fieldNames = append(fieldNames, acc.name)
	}
	sort.Strings(fieldNames)

	return s, fieldNames, s.universeCardinality(), nil
}

func (s *indexSnapshot) universeCardinality() uint64 {
	if s == nil {
		return 0
	}
	return s.universe.Cardinality()
}

func (db *DB[K, V]) collectPlannerFieldStatsFromOverlay(s *indexSnapshot, fieldName string) (PlannerFieldStats, error) {
	if err := db.unavailableErr(); err != nil {
		return PlannerFieldStats{}, err
	}

	ov := newFieldOverlayStorage(s.index[fieldName])
	if !ov.hasData() {
		return PlannerFieldStats{}, nil
	}
	return ov.fieldStats(), nil
}

func (ov fieldOverlay) fieldStats() PlannerFieldStats {
	var (
		total    uint64
		maxCard  uint64
		nonEmpty uint64
		distinct uint64
	)
	br := ov.rangeForBounds(rangeBounds{has: true})
	if br.baseStart >= br.baseEnd {
		return PlannerFieldStats{}
	}

	q50 := newP2Quantile(0.50)
	q95 := newP2Quantile(0.95)
	cur := ov.newCursor(br, false)
	for {
		_, ids, ok := cur.next()
		if !ok {
			break
		}
		card := ids.Cardinality()
		q50.observe(card)
		q95.observe(card)
		distinct++
		total += card
		if card > maxCard {
			maxCard = card
		}
		if card > 0 {
			nonEmpty++
		}
	}

	if distinct == 0 {
		return PlannerFieldStats{}
	}

	return PlannerFieldStats{
		DistinctKeys:    distinct,
		NonEmptyKeys:    nonEmpty,
		TotalBucketCard: total,
		AvgBucketCard:   float64(total) / float64(distinct),
		MaxBucketCard:   maxCard,
		P50BucketCard:   q50.value(),
		P95BucketCard:   q95.value(),
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
			if errors.Is(err, ErrClosed) || errors.Is(err, ErrBroken) {
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

func (db *DB[K, V]) plannerStatsSnapshotForPersistLocked(version uint64) *plannerStatsSnapshot {
	current := db.planner.stats.Load()
	if current == nil {
		return db.buildPlannerStatsSnapshotLocked(version)
	}

	return &plannerStatsSnapshot{
		Version:             version,
		GeneratedAt:         time.Now(),
		UniverseCardinality: db.getSnapshot().universeCardinality(),
		Fields:              current.Fields,
	}
}

func (db *DB[K, V]) buildPlannerStatsSnapshotLocked(version uint64) *plannerStatsSnapshot {
	snap := db.getSnapshot()
	fields := db.sortedPlannerFieldNames()

	out := &plannerStatsSnapshot{
		Version:             version,
		GeneratedAt:         time.Now(),
		UniverseCardinality: snap.universeCardinality(),
		Fields:              make(map[string]PlannerFieldStats, len(fields)),
	}

	for _, fieldName := range fields {
		ov := newFieldOverlayStorage(snap.index[fieldName])
		out.Fields[fieldName] = ov.fieldStats()
	}

	return out
}

func (db *DB[K, V]) sortedPlannerFieldNames() []string {
	if len(db.indexedFieldAccess) == 0 {
		return nil
	}
	fields := make([]string, 0, len(db.indexedFieldAccess))
	for _, acc := range db.indexedFieldAccess {
		fields = append(fields, acc.name)
	}
	sort.Strings(fields)
	return fields
}

func (db *DB[K, V]) publishLoadedPlannerStats(s *plannerStatsSnapshot) {
	if s == nil {
		return
	}
	out := &plannerStatsSnapshot{
		Version:             s.Version,
		GeneratedAt:         s.GeneratedAt,
		UniverseCardinality: db.getSnapshot().universeCardinality(),
		Fields:              make(map[string]PlannerFieldStats, len(db.indexedFieldAccess)),
	}
	if out.Version == 0 {
		out.Version = 1
	}
	if out.GeneratedAt.IsZero() {
		out.GeneratedAt = time.Now()
	}
	for _, f := range db.sortedPlannerFieldNames() {
		out.Fields[f] = s.Fields[f]
	}
	db.planner.statsVersion.Store(out.Version)
	db.planner.stats.Store(out)
}

type p2Quantile struct {
	q     float64
	count int
	init  [5]uint64
	n     [5]int
	np    [5]float64
	dn    [5]float64
	h     [5]float64
}

func newP2Quantile(q float64) p2Quantile {
	return p2Quantile{
		q: q,
		dn: [5]float64{
			0,
			q / 2,
			q,
			(1 + q) / 2,
			1,
		},
	}
}

func (q *p2Quantile) observe(v uint64) {
	if q.count < len(q.init) {
		q.init[q.count] = v
		q.count++
		if q.count == len(q.init) {
			sortSmallU64(q.init[:])
			for i := range q.h {
				q.h[i] = float64(q.init[i])
				q.n[i] = i + 1
			}
			q.np[0] = 1
			q.np[1] = 1 + 2*q.q
			q.np[2] = 1 + 4*q.q
			q.np[3] = 3 + 2*q.q
			q.np[4] = 5
		}
		return
	}

	q.count++

	var k int
	switch {
	case float64(v) < q.h[0]:
		q.h[0] = float64(v)
		k = 0
	case float64(v) < q.h[1]:
		k = 0
	case float64(v) < q.h[2]:
		k = 1
	case float64(v) < q.h[3]:
		k = 2
	case float64(v) <= q.h[4]:
		k = 3
	default:
		q.h[4] = float64(v)
		k = 3
	}

	for i := k + 1; i < len(q.n); i++ {
		q.n[i]++
	}
	for i := range q.np {
		q.np[i] += q.dn[i]
	}

	for i := 1; i <= 3; i++ {
		d := q.np[i] - float64(q.n[i])
		if d >= 1 && q.n[i+1]-q.n[i] > 1 {
			q.adjust(i, 1)
			continue
		}
		if d <= -1 && q.n[i-1]-q.n[i] < -1 {
			q.adjust(i, -1)
		}
	}
}

func (q *p2Quantile) value() uint64 {
	if q.count == 0 {
		return 0
	}
	if q.count < len(q.init) {
		var tmp [5]uint64
		copy(tmp[:], q.init[:q.count])
		sortSmallU64(tmp[:q.count])
		return percentileSmallU64(tmp[:q.count], q.q)
	}
	if q.count == len(q.init) {
		return percentileSmallU64(q.init[:], q.q)
	}
	if q.h[2] <= 0 {
		return 0
	}
	return uint64(q.h[2] + 0.5)
}

func (q *p2Quantile) adjust(i, step int) {
	next := q.parabolic(i, step)
	if q.h[i-1] < next && next < q.h[i+1] {
		q.h[i] = next
	} else {
		q.h[i] = q.linear(i, step)
	}
	q.n[i] += step
}

func (q *p2Quantile) parabolic(i, step int) float64 {
	nim1 := float64(q.n[i-1])
	ni := float64(q.n[i])
	nip1 := float64(q.n[i+1])
	stepf := float64(step)
	return q.h[i] + stepf/(nip1-nim1)*((ni-nim1+stepf)*(q.h[i+1]-q.h[i])/(nip1-ni)+(nip1-ni-stepf)*(q.h[i]-q.h[i-1])/(ni-nim1))
}

func (q *p2Quantile) linear(i, step int) float64 {
	j := i + step
	return q.h[i] + float64(step)*(q.h[j]-q.h[i])/float64(q.n[j]-q.n[i])
}

func sortSmallU64(v []uint64) {
	for i := 1; i < len(v); i++ {
		x := v[i]
		j := i
		for j > 0 && v[j-1] > x {
			v[j] = v[j-1]
			j--
		}
		v[j] = x
	}
}

func percentileSmallU64(sorted []uint64, q float64) uint64 {
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
	pos := int(math.Ceil(q*float64(n))) - 1
	if pos < 0 {
		pos = 0
	}
	if pos >= n {
		pos = n - 1
	}
	return sorted[pos]
}
