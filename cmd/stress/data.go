package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi"
	"github.com/vapstack/rbi/internal/mathutil"
	"github.com/vapstack/rbi/rbitrace"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

var (
	Countries = []string{"US", "CA", "GB", "DE", "FR", "NL", "PL", "SE", "JP", "IN", "BR", "AU"}
	Plans     = []string{"free", "starter", "pro", "enterprise"}
	Statuses  = []string{"active", "inactive", "pending", "suspended", "banned"}
	AllTags   = []string{
		"technology", "programming", "golang", "rust", "linux", "webdev", "frontend", "backend",
		"databases", "ai", "machine-learning", "datascience", "security", "devops", "cloud", "kubernetes",
		"startups", "careeradvice", "productivity", "design", "gaming", "android", "ios", "photography",
		"music", "movies", "books", "history", "science", "space", "economics", "politics",
		"sports", "soccer", "basketball", "fitness", "travel", "food", "finance", "cryptocurrency",
	}
	AllRoles = []string{"member", "trusted", "moderator", "admin", "staff", "bot"}
)

const (
	stressBoltOpenTimeout = 500 * time.Millisecond
	seedBatchSize         = 5_000
)

type DBConfig struct {
	DBFile               string
	SeedRecords          int
	SeedRecordsSet       bool
	OpenTimeout          time.Duration
	BoltNoSync           bool
	AnalyzeInterval      time.Duration
	DisableRuntimeCaches bool
	TraceSink            func(rbitrace.Event)
	TraceSampleEvery     int
}

type CollectionHandle struct {
	Collection   *rbi.Collection[uint64, UserBench]
	RawBolt      *bolt.DB
	StartRecords uint64
	MaxID        uint64
	EmailSamples []string
	DBFile       string
}

func buildRBIOptions(cfg DBConfig) rbi.Options {
	var dbOpts rbi.Options
	if cfg.DisableRuntimeCaches {
		dbOpts.MaterializedPredicateCacheMaxEntries = -1
		dbOpts.NumericRangeBucketSize = -1
		dbOpts.NumericRangeBucketMinFieldKeys = -1
		dbOpts.NumericRangeBucketMinSpanKeys = -1
	}
	if cfg.AnalyzeInterval != 0 {
		dbOpts.AnalyzeInterval = cfg.AnalyzeInterval
	}
	if cfg.TraceSink != nil {
		dbOpts.TraceSink = cfg.TraceSink
		dbOpts.TraceSampleEvery = cfg.TraceSampleEvery
	}
	return dbOpts
}

func OpenBenchDB(cfg DBConfig, emailSampleN int) (*CollectionHandle, error) {
	if cfg.DBFile == "" {
		cfg.DBFile = DefaultDBFilename
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = stressBoltOpenTimeout
	}
	if abs, err := filepath.Abs(cfg.DBFile); err == nil {
		cfg.DBFile = abs
	}
	dbOpts := buildRBIOptions(cfg)
	rawBolt, err := bolt.Open(cfg.DBFile, 0o600, &bolt.Options{
		NoSync:  cfg.BoltNoSync,
		Timeout: cfg.OpenTimeout,
	})
	if err != nil {
		if errors.Is(err, bolterrors.ErrTimeout) {
			return nil, fmt.Errorf(
				"open bolt db: lock timeout after %s for %s (another stress or bench process is likely still using this DB)",
				cfg.OpenTimeout,
				cfg.DBFile,
			)
		}
		return nil, fmt.Errorf("open bolt db: %w", err)
	}
	rbi.EnableStoreStats = true
	c, err := rbi.Open[uint64, UserBench](rawBolt, dbOpts)
	if err != nil {
		_ = rawBolt.Close()
		return nil, fmt.Errorf("init rbi: %w", err)
	}

	startRecords, maxID, err := loadOrSeedCollection(c, cfg.SeedRecords, cfg.SeedRecordsSet)
	if err != nil {
		_ = c.Close()
		_ = rawBolt.Close()
		return nil, err
	}
	emailSamples := buildEmailSample(c, maxID, emailSampleN)
	return &CollectionHandle{
		Collection:   c,
		RawBolt:      rawBolt,
		StartRecords: startRecords,
		MaxID:        maxID,
		EmailSamples: emailSamples,
		DBFile:       cfg.DBFile,
	}, nil
}

func (h *CollectionHandle) Close() error {
	var err error
	if h == nil {
		return nil
	}
	if h.Collection != nil {
		err = h.Collection.Close()
	}
	if h.RawBolt != nil {
		if closeErr := h.RawBolt.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func loadOrSeedCollection(c *rbi.Collection[uint64, UserBench], seedRecords int, seedRecordsSet bool) (uint64, uint64, error) {
	count, err := stressReadCount(c)
	if err != nil {
		return 0, 0, fmt.Errorf("count existing records: %w", err)
	}
	target := uint64(InitialRecords)
	if seedRecordsSet {
		target = uint64(seedRecords)
	}
	var maxID uint64
	if count > 0 {
		stats, err := c.Stats()
		if err != nil {
			return 0, 0, fmt.Errorf("stats: %w", err)
		}
		maxID = stats.LastKey
		if maxID == 0 {
			maxID = scanMaxID(c)
		}
		if count >= target {
			return count, maxID, nil
		}
		missing := target - count
		log.Printf("Collection has %d records. Seeding %d more to reach %d...", count, missing, target)
		seedData(c, &maxID, int(missing))
		count, err = stressReadCount(c)
		if err != nil {
			return 0, 0, fmt.Errorf("count after top-up seed: %w", err)
		}
		return count, maxID, nil
	}

	if target == 0 {
		return 0, 0, nil
	}
	log.Printf("Collection is empty. Seeding %d records...", target)
	seedData(c, &maxID, int(target))
	count, err = stressReadCount(c)
	if err != nil {
		return 0, 0, fmt.Errorf("count after seed: %w", err)
	}
	return count, maxID, nil
}

func scanMaxID(c *rbi.Collection[uint64, UserBench]) uint64 {
	var maxID uint64
	_ = stressReadScanKeys(c, 0, func(k uint64) (bool, error) {
		if k > maxID {
			maxID = k
		}
		return true, nil
	})
	return maxID
}

func buildEmailSample(c *rbi.Collection[uint64, UserBench], maxID uint64, size int) []string {
	if maxID == 0 || size <= 0 {
		return nil
	}

	seen := make(map[string]struct{}, size)
	emails := make([]string, 0, size)
	rng := mathutil.NewRand(1)

	attempts := size * 40
	if attempts < 5000 {
		attempts = 5000
	}

	for i := 0; i < attempts && len(emails) < size; i++ {
		id := pickUniformID(rng, maxID)
		rec, err := stressReadGet(c, id)
		if err != nil {
			c.ReleaseRecords(rec)
			continue
		}
		if rec == nil {
			continue
		}
		email := rec.Email
		c.ReleaseRecords(rec)
		if email == "" {
			continue
		}
		if _, ok := seen[email]; ok {
			continue
		}
		seen[email] = struct{}{}
		emails = append(emails, email)
	}

	return emails
}

func seedData(c *rbi.Collection[uint64, UserBench], maxID *uint64, count int) {
	rng := mathutil.NewRand(time.Now().UnixNano())

	ids := make([]uint64, 0, seedBatchSize)
	users := make([]*UserBench, 0, seedBatchSize)

	for i := 0; i < count; i += seedBatchSize {
		size := seedBatchSize
		if i+size > count {
			size = count - i
		}
		ids = ids[:size]
		users = users[:size]
		for j := 0; j < size; j++ {
			id := atomic.AddUint64(maxID, 1)
			ids[j] = id
			users[j] = generateUser(rng, id)
		}
		if err := stressWriteSets(c, ids, users); err != nil {
			log.Fatalf("Seeding failed: %v", err)
		}
		clear(users)

		if i > 0 && i%500_000 == 0 {
			log.Printf("Seeded %d records...", i)
		}
	}
}

func generateUser(rng *rand.Rand, id uint64) *UserBench {
	now := time.Now()
	var createdAt int64
	switch r := rng.Float64(); {
	case r < 0.08:
		createdAt = now.Add(-time.Duration(rng.IntN(24)) * time.Hour).Unix()
	case r < 0.35:
		createdAt = now.Add(-time.Duration(rng.IntN(30*24)) * time.Hour).Unix()
	case r < 0.80:
		createdAt = now.Add(-time.Duration(rng.IntN(365*24)) * time.Hour).Unix()
	default:
		createdAt = now.Add(-time.Duration(rng.IntN(4*365*24)) * time.Hour).Unix()
	}

	var lastLogin int64
	switch r := rng.Float64(); {
	case r < 0.52:
		lastLogin = now.Add(-time.Duration(rng.IntN(24)) * time.Hour).Unix()
	case r < 0.82:
		lastLogin = now.Add(-time.Duration(rng.IntN(14*24)) * time.Hour).Unix()
	case r < 0.95:
		lastLogin = now.Add(-time.Duration(rng.IntN(90*24)) * time.Hour).Unix()
	default:
		lastLogin = now.Add(-time.Duration(rng.IntN(3*365*24)) * time.Hour).Unix()
	}

	namePrefixes := []string{"u_", "user_", "dev_", "mod_", "news_", "anon_"}
	namePrefix := namePrefixes[rng.IntN(len(namePrefixes))]

	return &UserBench{
		ID:         id,
		Name:       fmt.Sprintf("%s%x_%d", namePrefix, id, rng.IntN(10_000)),
		Email:      fmt.Sprintf("user%d_%d@example.com", id, rng.IntN(1_000_000)),
		Country:    Countries[rng.IntN(len(Countries))],
		Plan:       weightedPlan(rng),
		Status:     weightedStatus(rng),
		Age:        13 + rng.IntN(58),
		Score:      sampleScore(rng),
		IsVerified: rng.Float64() < 0.58,
		CreatedAt:  createdAt,
		LastLogin:  lastLogin,
		Tags:       pickUserTags(rng),
		Roles:      sampleRoleSet(rng),
		Blob:       make([]byte, 96+rng.IntN(96)),
	}
}

func pickSampleEmail(rng *rand.Rand, emails []string) string {
	if len(emails) == 0 {
		return ""
	}
	hot := len(emails) / 5
	if hot < 1 {
		hot = 1
	}
	if rng.Float64() < 0.70 {
		return emails[rng.IntN(hot)]
	}
	return emails[rng.IntN(len(emails))]
}

func pickUniformID(rng *rand.Rand, maxID uint64) uint64 {
	if maxID == 0 {
		return 0
	}
	return uint64(rng.Int64N(int64(maxID))) + 1
}

func pickReadID(rng *rand.Rand, maxID uint64) uint64 {
	if maxID == 0 {
		return 0
	}
	const hotWindow = uint64(200_000)
	if maxID <= hotWindow {
		return pickUniformID(rng, maxID)
	}
	if rng.Float64() < 0.75 {
		start := maxID - hotWindow + 1
		return start + uint64(rng.Int64N(int64(hotWindow)))
	}
	return pickUniformID(rng, maxID)
}

func weightedPlan(rng *rand.Rand) string {
	r := rng.Float64()
	switch {
	case r < 0.82:
		return "free"
	case r < 0.93:
		return "starter"
	case r < 0.99:
		return "pro"
	default:
		return "enterprise"
	}
}

func weightedStatus(rng *rand.Rand) string {
	r := rng.Float64()
	switch {
	case r < 0.72:
		return "active"
	case r < 0.90:
		return "inactive"
	case r < 0.96:
		return "pending"
	case r < 0.99:
		return "suspended"
	default:
		return "banned"
	}
}

func sampleScore(rng *rand.Rand) float64 {
	base := rng.Float64() * 180
	switch r := rng.Float64(); {
	case r < 0.55:
		base += rng.Float64() * 220
	case r < 0.85:
		base += rng.Float64() * 900
	case r < 0.97:
		base += rng.Float64() * 4500
	default:
		base += rng.Float64() * 22000
	}
	return base
}

func pickUserTags(rng *rand.Rand) []string {
	if len(AllTags) == 0 {
		return nil
	}
	n := 1 + rng.IntN(4)
	if rng.Float64() < 0.08 {
		n = 5
	}
	if n > len(AllTags) {
		n = len(AllTags)
	}
	hotSpan := len(AllTags) / 3
	if hotSpan < 1 {
		hotSpan = 1
	}
	out := make([]string, 0, n)
	for len(out) < n {
		var tag string
		if rng.Float64() < 0.72 {
			tag = AllTags[rng.IntN(hotSpan)]
		} else {
			tag = AllTags[rng.IntN(len(AllTags))]
		}
		if stringSliceContains(out, tag) {
			continue
		}
		out = append(out, tag)
	}
	return out
}

func sampleRoleSet(rng *rand.Rand) []string {
	n := 1
	if rng.Float64() < 0.14 {
		n = 2
	}
	if rng.Float64() < 0.02 {
		n = 3
	}
	if n > len(AllRoles) {
		n = len(AllRoles)
	}
	out := make([]string, 0, n)
	for len(out) < n {
		role := AllRoles[rng.IntN(len(AllRoles))]
		if stringSliceContains(out, role) {
			continue
		}
		out = append(out, role)
	}
	return out
}

func pickRandomSlice(rng *rand.Rand, values []string, n int) []string {
	if len(values) == 0 || n <= 0 {
		return nil
	}
	if n > len(values) {
		n = len(values)
	}
	out := make([]string, 0, n)
	for len(out) < n {
		v := values[rng.IntN(len(values))]
		if stringSliceContains(out, v) {
			continue
		}
		out = append(out, v)
	}
	return out
}

func stringSliceContains(values []string, v string) bool {
	for i := range values {
		if values[i] == v {
			return true
		}
	}
	return false
}
