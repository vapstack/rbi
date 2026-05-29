package persist

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/vapstack/rbi/internal/qexec"
)

func writePlannerStatsSnapshot(writer *bufio.Writer, s *qexec.PlannerStatsSnapshot) error {
	if s == nil {
		if err := writeSidecarUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing planner stats version: %w", err)
		}
		if err := writeSidecarUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing planner stats generated_at: %w", err)
		}
		if err := writeSidecarUvarint(writer, 0); err != nil {
			return fmt.Errorf("encode: writing planner stats universe: %w", err)
		}
		return writeSidecarUvarint(writer, 0)
	}

	if err := writeSidecarUvarint(writer, s.Version); err != nil {
		return fmt.Errorf("encode: writing planner stats version: %w", err)
	}

	generatedAt := uint64(0)
	if !s.GeneratedAt.IsZero() {
		generatedAt = uint64(s.GeneratedAt.UnixNano())
	}
	if err := writeSidecarUvarint(writer, generatedAt); err != nil {
		return fmt.Errorf("encode: writing planner stats generated_at: %w", err)
	}
	if err := writeSidecarUvarint(writer, s.UniverseCardinality); err != nil {
		return fmt.Errorf("encode: writing planner stats universe: %w", err)
	}

	fields := sortedMapPlannerFieldNames(s.Fields)
	if err := writeSidecarUvarint(writer, uint64(len(fields))); err != nil {
		return fmt.Errorf("encode: writing planner stats field count: %w", err)
	}
	for _, f := range fields {
		if err := writeSidecarString(writer, f); err != nil {
			return fmt.Errorf("encode: writing planner stats field name: %w", err)
		}
		if err := writePlannerFieldStats(writer, s.Fields[f]); err != nil {
			return fmt.Errorf("encode: writing planner stats field %q: %w", f, err)
		}
	}
	return nil
}

func readPlannerStatsSnapshot(reader *bufio.Reader, compatible map[string]bool) (*qexec.PlannerStatsSnapshot, error) {
	version, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats version: %w", err)
	}
	generatedAtNanos, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats generated_at: %w", err)
	}
	universe, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats universe: %w", err)
	}
	fieldCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("decode: reading planner stats field count: %w", err)
	}
	if fieldCount > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("decode: planner stats field count overflows int: %v", fieldCount)
	}
	if version == 0 && generatedAtNanos == 0 && universe == 0 && fieldCount == 0 {
		return nil, nil
	}

	fields := make(map[string]qexec.PlannerFieldStats, min(int(fieldCount), len(compatible)))
	for i := uint64(0); i < fieldCount; i++ {
		f, err := readSidecarString(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading planner stats field name %d/%d: %w", i+1, fieldCount, err)
		}
		stats, err := readPlannerFieldStats(reader)
		if err != nil {
			return nil, fmt.Errorf("decode: reading planner stats field %q (%d/%d): %w", f, i+1, fieldCount, err)
		}
		if compatible[f] {
			if _, exists := fields[f]; exists {
				return nil, fmt.Errorf("decode: duplicate planner stats field %q at entry %d/%d", f, i+1, fieldCount)
			}
			fields[f] = stats
		}
	}
	if version == 0 && generatedAtNanos == 0 && universe == 0 && len(fields) == 0 {
		return nil, nil
	}
	for f := range compatible {
		if _, ok := fields[f]; !ok {
			return nil, fmt.Errorf("decode: missing planner stats field %q (loaded=%d compatible=%d)", f, len(fields), len(compatible))
		}
	}

	out := &qexec.PlannerStatsSnapshot{
		Version:             version,
		UniverseCardinality: universe,
		Fields:              fields,
	}
	if generatedAtNanos > 0 {
		out.GeneratedAt = time.Unix(0, int64(generatedAtNanos)).UTC()
	}
	return out, nil
}

func writePlannerFieldStats(writer *bufio.Writer, s qexec.PlannerFieldStats) error {
	if err := writeSidecarUvarint(writer, s.DistinctKeys); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.NonEmptyKeys); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.TotalBucketCard); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.MaxBucketCard); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.P50BucketCard); err != nil {
		return err
	}
	if err := writeSidecarUvarint(writer, s.P95BucketCard); err != nil {
		return err
	}
	return nil
}

func readPlannerFieldStats(reader *bufio.Reader) (qexec.PlannerFieldStats, error) {
	distinct, err := binary.ReadUvarint(reader)
	if err != nil {
		return qexec.PlannerFieldStats{}, err
	}
	nonEmpty, err := binary.ReadUvarint(reader)
	if err != nil {
		return qexec.PlannerFieldStats{}, err
	}
	total, err := binary.ReadUvarint(reader)
	if err != nil {
		return qexec.PlannerFieldStats{}, err
	}
	maxCard, err := binary.ReadUvarint(reader)
	if err != nil {
		return qexec.PlannerFieldStats{}, err
	}
	p50, err := binary.ReadUvarint(reader)
	if err != nil {
		return qexec.PlannerFieldStats{}, err
	}
	p95, err := binary.ReadUvarint(reader)
	if err != nil {
		return qexec.PlannerFieldStats{}, err
	}

	out := qexec.PlannerFieldStats{
		DistinctKeys:    distinct,
		NonEmptyKeys:    nonEmpty,
		TotalBucketCard: total,
		MaxBucketCard:   maxCard,
		P50BucketCard:   p50,
		P95BucketCard:   p95,
	}
	if distinct > 0 {
		out.AvgBucketCard = float64(total) / float64(distinct)
	}
	return out, nil
}
