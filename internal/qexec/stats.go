package qexec

import (
	"math"
	"sort"
	"time"

	"github.com/vapstack/rbi/internal/indexdata"
	"github.com/vapstack/rbi/internal/snapshot"
)

func (r *Runtime) RefreshPlannerStatsOnSnapshot(snap *snapshot.View) {
	fieldNames := r.sortedPlannerFieldNames()
	universeCardinality := snap.UniverseCardinality()

	out := PlannerStatsSnapshot{
		GeneratedAt:         time.Now(),
		UniverseCardinality: universeCardinality,
		Fields:              make(map[string]PlannerFieldStats, len(fieldNames)),
	}

	for _, fieldName := range fieldNames {
		out.Fields[fieldName] = r.collectPlannerFieldStatsFromIndexView(snap, fieldName)
	}

	out.Version = r.StatsVersion.Add(1)
	r.Stats.Store(&out)
}

func (r *Runtime) PlannerStatsSnapshotForPersist(snap *snapshot.View, version uint64) *PlannerStatsSnapshot {
	current := r.Stats.Load()
	if current == nil {
		return r.BuildPlannerStatsSnapshot(snap, version)
	}
	return &PlannerStatsSnapshot{
		Version:             version,
		GeneratedAt:         time.Now(),
		UniverseCardinality: snap.UniverseCardinality(),
		Fields:              current.Fields,
	}
}

func (r *Runtime) BuildPlannerStatsSnapshot(snap *snapshot.View, version uint64) *PlannerStatsSnapshot {
	fields := r.sortedPlannerFieldNames()
	out := &PlannerStatsSnapshot{
		Version:             version,
		GeneratedAt:         time.Now(),
		UniverseCardinality: snap.UniverseCardinality(),
		Fields:              make(map[string]PlannerFieldStats, len(fields)),
	}

	for _, fieldName := range fields {
		out.Fields[fieldName] = r.collectPlannerFieldStatsFromIndexView(snap, fieldName)
	}

	return out
}

func (r *Runtime) PublishLoadedPlannerStats(s *PlannerStatsSnapshot, snap *snapshot.View) {
	out := &PlannerStatsSnapshot{
		Version:             s.Version,
		GeneratedAt:         s.GeneratedAt,
		UniverseCardinality: snap.UniverseCardinality(),
		Fields:              make(map[string]PlannerFieldStats, len(r.Schema.Indexed)),
	}
	if out.Version == 0 {
		out.Version = 1
	}
	if out.GeneratedAt.IsZero() {
		out.GeneratedAt = time.Now()
	}
	for _, f := range r.sortedPlannerFieldNames() {
		out.Fields[f] = s.Fields[f]
	}
	r.StatsVersion.Store(out.Version)
	r.Stats.Store(out)
}

func (r *Runtime) collectPlannerFieldStatsFromIndexView(s *snapshot.View, fieldName string) PlannerFieldStats {
	acc, ok := r.Schema.IndexedByName[fieldName]
	if !ok || s.Index == nil || acc.Ordinal >= len(s.Index) {
		return PlannerFieldStats{}
	}
	ov := indexdata.NewFieldIndexViewFromStorage(s.Index[acc.Ordinal])
	if !ov.HasData() {
		return PlannerFieldStats{}
	}
	return plannerFieldIndexViewStats(ov)
}

func plannerFieldIndexViewStats(o indexdata.FieldIndexView) PlannerFieldStats {
	keyCount := uint64(o.KeyCount())
	if keyCount == 0 {
		return PlannerFieldStats{}
	}
	if o.Rows() == keyCount {
		return PlannerFieldStats{
			DistinctKeys:    keyCount,
			NonEmptyKeys:    keyCount,
			TotalBucketCard: keyCount,
			AvgBucketCard:   1,
			MaxBucketCard:   1,
			P50BucketCard:   1,
			P95BucketCard:   1,
		}
	}

	var (
		total    uint64
		maxCard  uint64
		nonEmpty uint64
		distinct uint64
	)
	br := o.RangeForBounds(indexdata.Bounds{Has: true})
	if br.BaseStart >= br.BaseEnd {
		return PlannerFieldStats{}
	}

	q50 := newP2Quantile(0.50)
	q95 := newP2Quantile(0.95)
	cur := o.NewCursor(br, false)
	for {
		ids, _, single, ok := cur.NextPostingOrSingle()
		if !ok {
			break
		}
		card := uint64(1)
		if !single {
			card = ids.Cardinality()
		}
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

func (r *Runtime) sortedPlannerFieldNames() []string {
	if len(r.Schema.Indexed) == 0 {
		return nil
	}
	fields := make([]string, 0, len(r.Schema.Indexed))
	for _, acc := range r.Schema.Indexed {
		fields = append(fields, acc.Name)
	}
	sort.Strings(fields)
	return fields
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
