package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync/atomic"
	"syscall"

	"github.com/vapstack/rbi"
	bolt "go.etcd.io/bbolt"
)

func main() {
	opts := parseBenchOptions()
	opts.applySnapshotOverrides()

	defer log.Println("Done")

	stopProfiling, err := startBenchProfiling(opts)
	if err != nil {
		log.Fatalf("Failed to initialize profiling: %v", err)
	}
	defer func() {
		if stopProfiling == nil {
			return
		}
		if err = stopProfiling(); err != nil {
			log.Printf("Profiling finalize error: %v\n", err)
		}
	}()

	db, rawBolt := openBenchDatabase(opts)
	defer func() {
		if err = db.Close(); err != nil {
			log.Printf("Close error: %v\n", err)
		}
		if err = rawBolt.Close(); err != nil {
			log.Printf("Bolt close error: %v\n", err)
		}
	}()

	startCount, maxID := loadOrSeedDatabase(db)
	emailSamples := buildEmailSamples(db, maxID, opts.emailSampleN)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interrupted, stopSignals := installSignalCancel(cancel)
	defer stopSignals()

	var report BenchmarkReport
	switch {
	case opts.matrix:
		report = buildMatrixModeReport(ctx, db, &maxID, startCount, emailSamples, opts, interrupted)
	case opts.sweep:
		report = buildSweepModeReport(ctx, db, maxID, startCount, emailSamples, opts)
	case opts.stress:
		report = buildStressModeReport(ctx, db, &maxID, startCount, emailSamples, opts)
	case opts.ceiling:
		report = buildCeilingModeReport(ctx, db, &maxID, startCount, emailSamples, opts)
	default:
		log.Fatal("no mode selected")
	}

	if err = saveJSON(opts.outFile, report); err != nil {
		log.Fatalf("Failed to save report: %v", err)
	}
	if err = saveJSON(opts.outFile+".stats.json", collectBenchDiagnostics(db)); err != nil {
		log.Fatalf("Failed to save stats: %v", err)
	}
	switch report.Mode {
	case "sweep":
		log.Printf("Sweep mode finished. Results saved to %s", opts.outFile)
	case "stress":
		log.Printf("Stress mode finished. Results saved to %s", opts.outFile)
	case "ceiling":
		log.Printf("Ceiling mode finished. Results saved to %s", opts.outFile)
	case "matrix":
		log.Printf("Benchmark matrix finished. Results saved to %s", opts.outFile)
	default:
		log.Printf("Benchmark finished. Results saved to %s", opts.outFile)
	}
}

func openBenchDatabase(opts benchOptions) (*rbi.DB[uint64, UserBench], *bolt.DB) {
	log.Println("Opening database...")
	dbOpts := rbi.Options{}
	if opts.analyzeInterval != 0 {
		dbOpts.AnalyzeInterval = opts.analyzeInterval
	}

	registryLabel := "default"
	fieldKeysLabel := "default"
	fieldOpsLabel := "default"
	maxFieldsLabel := "default"
	universeOpsLabel := "default"
	layerDepthLabel := "default"
	analyzeLabel := "default"
	if opts.analyzeInterval < 0 {
		analyzeLabel = "disabled"
	} else if opts.analyzeInterval > 0 {
		analyzeLabel = opts.analyzeInterval.String()
	}

	if opts.snapshotRegistryMax >= 0 {
		dbOpts.SnapshotRegistryMax = opts.snapshotRegistryMax
		registryLabel = strconv.Itoa(opts.snapshotRegistryMax)
	}
	if opts.snapshotDeltaCompactFieldKeys >= 0 {
		dbOpts.SnapshotDeltaCompactFieldKeys = opts.snapshotDeltaCompactFieldKeys
		fieldKeysLabel = strconv.Itoa(opts.snapshotDeltaCompactFieldKeys)
	}
	if opts.snapshotDeltaCompactFieldOps >= 0 {
		dbOpts.SnapshotDeltaCompactFieldOps = opts.snapshotDeltaCompactFieldOps
		fieldOpsLabel = strconv.Itoa(opts.snapshotDeltaCompactFieldOps)
	}
	if opts.snapshotDeltaCompactMaxFieldsPublish != -1 {
		dbOpts.SnapshotDeltaCompactMaxFieldsPerPublish = opts.snapshotDeltaCompactMaxFieldsPublish
		maxFieldsLabel = strconv.Itoa(opts.snapshotDeltaCompactMaxFieldsPublish)
	}
	if opts.snapshotDeltaCompactUniverseOps >= 0 {
		dbOpts.SnapshotDeltaCompactUniverseOps = opts.snapshotDeltaCompactUniverseOps
		universeOpsLabel = strconv.Itoa(opts.snapshotDeltaCompactUniverseOps)
	}
	if opts.snapshotDeltaLayerMaxDepth >= 0 {
		dbOpts.SnapshotDeltaLayerMaxDepth = opts.snapshotDeltaLayerMaxDepth
		layerDepthLabel = strconv.Itoa(opts.snapshotDeltaLayerMaxDepth)
	}

	// dbOpts.AutoBatchMax = 1024
	// dbOpts.AutoBatchWindow = 50 * time.Microsecond
	// dbOpts.AutoBatchMaxQueue = 2048

	log.Printf(
		"Snapshot config: registry=%s field_keys=%s field_ops=%s max_fields_per_publish=%s universe_ops=%s layer_depth=%s analyze_interval=%s",
		registryLabel,
		fieldKeysLabel,
		fieldOpsLabel,
		maxFieldsLabel,
		universeOpsLabel,
		layerDepthLabel,
		analyzeLabel,
	)
	if opts.boltNoSync {
		log.Printf("Bolt NoSync enabled (unsafe durability mode)")
	}
	if EnableSnapshotDiagnosticsLogs {
		log.Printf("Snapshot diagnostics progress logging enabled")
	}

	rawBolt, err := bolt.Open(DBFilename, 0o600, &bolt.Options{NoSync: opts.boltNoSync})
	if err != nil {
		log.Fatalf("Failed to open bolt db: %v", err)
	}
	db, err := rbi.New[uint64, UserBench](rawBolt, dbOpts)
	if err != nil {
		_ = rawBolt.Close()
		log.Fatalf("Failed to initialize db: %v", err)
	}
	return db, rawBolt
}

func loadOrSeedDatabase(db *rbi.DB[uint64, UserBench]) (uint64, uint64) {
	count, _ := db.Count(nil)
	var maxID uint64

	if count > 0 {
		stats := db.IndexStats()
		maxID = stats.LastKey
		if maxID == 0 {
			maxID = scanMaxID(db)
		}
		log.Printf("Found %d existing records. maxID=%d", count, maxID)
		return count, maxID
	}

	log.Printf("Database empty. Seeding %d records...", InitialRecords)
	seedData(db, &maxID, InitialRecords)
	count, _ = db.Count(nil)
	log.Printf("Seeding finished. records=%d maxID=%d", count, maxID)
	return count, maxID
}

func buildEmailSamples(db *rbi.DB[uint64, UserBench], maxID uint64, sampleN int) []string {
	emails := buildEmailSample(db, maxID, sampleN)
	log.Printf("Collected %d sampled emails for single-record query profile", len(emails))
	return emails
}

func installSignalCancel(cancel context.CancelFunc) (*atomic.Bool, func()) {
	interrupted := new(atomic.Bool)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		s := <-sigChan
		interrupted.Store(true)
		log.Printf("Received %s, stopping benchmark...", s.String())
		cancel()
	}()

	return interrupted, func() {
		signal.Stop(sigChan)
	}
}

const randStreamMix uint64 = 0x9e3779b97f4a7c15

func newRand(seed int64) *rand.Rand {
	s := uint64(seed)
	return rand.New(rand.NewPCG(s, s^randStreamMix))
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

func saveJSON(path string, v any) error {
	file, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, file, 0o644)
}
