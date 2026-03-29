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

const randStreamMix uint64 = 0x9e3779b97f4a7c15

const stressBoltOpenTimeout = 500 * time.Millisecond

// Keep this timeout fixed and short in stress.
// Do not make it user-configurable and do not increase it.
// Stress/bench helpers must not run in parallel against the same DB file; agents
// are expected to wait for full process exit and DB close instead of masking
// that mistake with a long lock wait here.

type DBConfig struct {
	DBFile           string
	OpenTimeout      time.Duration
	BoltNoSync       bool
	AnalyzeInterval  time.Duration
	CalibrationOn    bool
	CalibrationEvery int
	TraceSink        func(rbi.TraceEvent)
	TraceSampleEvery int
}

type DBHandle struct {
	DB           *rbi.DB[uint64, UserBench]
	RawBolt      *bolt.DB
	StartRecords uint64
	MaxID        uint64
	EmailSamples []string
	DBFile       string
}

func buildRBIOptions(cfg DBConfig) rbi.Options {
	dbOpts := rbi.Options{
		EnableAutoBatchStats: true,
		EnableSnapshotStats:  true,
	}
	if cfg.AnalyzeInterval != 0 {
		dbOpts.AnalyzeInterval = cfg.AnalyzeInterval
	}
	if cfg.CalibrationOn {
		dbOpts.CalibrationEnabled = true
		dbOpts.CalibrationSampleEvery = cfg.CalibrationEvery
	}
	if cfg.TraceSink != nil {
		dbOpts.TraceSink = cfg.TraceSink
		dbOpts.TraceSampleEvery = cfg.TraceSampleEvery
	}
	return dbOpts
}

func OpenBenchDB(cfg DBConfig, emailSampleN int) (*DBHandle, error) {
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
	db, err := rbi.New[uint64, UserBench](rawBolt, dbOpts)
	if err != nil {
		_ = rawBolt.Close()
		return nil, fmt.Errorf("init rbi: %w", err)
	}

	startRecords, maxID, err := loadOrSeedDatabase(db)
	if err != nil {
		_ = db.Close()
		_ = rawBolt.Close()
		return nil, err
	}
	emailSamples := buildEmailSample(db, maxID, emailSampleN)
	return &DBHandle{
		DB:           db,
		RawBolt:      rawBolt,
		StartRecords: startRecords,
		MaxID:        maxID,
		EmailSamples: emailSamples,
		DBFile:       cfg.DBFile,
	}, nil
}

func (h *DBHandle) Close() error {
	var err error
	if h == nil {
		return nil
	}
	if h.DB != nil {
		err = h.DB.Close()
	}
	if h.RawBolt != nil {
		if closeErr := h.RawBolt.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func loadOrSeedDatabase(db *rbi.DB[uint64, UserBench]) (uint64, uint64, error) {
	count, err := db.Count(nil)
	if err != nil {
		return 0, 0, fmt.Errorf("count existing records: %w", err)
	}
	var maxID uint64
	if count > 0 {
		stats := db.Stats()
		maxID = stats.LastKey
		if maxID == 0 {
			maxID = scanMaxID(db)
		}
		return count, maxID, nil
	}

	log.Printf("Database empty. Seeding %d records...", InitialRecords)
	seedData(db, &maxID, InitialRecords)
	count, err = db.Count(nil)
	if err != nil {
		return 0, 0, fmt.Errorf("count after seed: %w", err)
	}
	return count, maxID, nil
}

func scanMaxID(db *rbi.DB[uint64, UserBench]) uint64 {
	var maxID uint64
	_ = db.ScanKeys(0, func(k uint64) (bool, error) {
		if k > maxID {
			maxID = k
		}
		return true, nil
	})
	return maxID
}

func buildEmailSample(db *rbi.DB[uint64, UserBench], maxID uint64, size int) []string {
	if maxID == 0 || size <= 0 {
		return nil
	}

	seen := make(map[string]struct{}, size)
	emails := make([]string, 0, size)
	rng := NewRand(1)

	attempts := size * 40
	if attempts < 5000 {
		attempts = 5000
	}

	for i := 0; i < attempts && len(emails) < size; i++ {
		id := pickUniformID(rng, maxID)
		rec, err := db.Get(id)
		if err != nil {
			db.ReleaseRecords(rec)
			continue
		}
		if rec == nil {
			continue
		}
		email := rec.Email
		db.ReleaseRecords(rec)
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

func seedData(db *rbi.DB[uint64, UserBench], maxID *uint64, count int) {
	rng := NewRand(time.Now().UnixNano())
	batchSize := 50_000

	for i := 0; i < count; i += batchSize {
		size := batchSize
		if i+size > count {
			size = count - i
		}
		ids := make([]uint64, size)
		users := make([]*UserBench, size)
		for j := 0; j < size; j++ {
			id := atomic.AddUint64(maxID, 1)
			ids[j] = id
			users[j] = generateUser(rng, id)
		}
		if err := db.BatchSet(ids, users); err != nil {
			log.Fatalf("Seeding failed: %v", err)
		}
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

func NewRand(seed int64) *rand.Rand {
	s := uint64(seed)
	return rand.New(rand.NewPCG(s, s^randStreamMix))
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
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for len(out) < n {
		var tag string
		if rng.Float64() < 0.72 {
			tag = AllTags[rng.IntN(hotSpan)]
		} else {
			tag = AllTags[rng.IntN(len(AllTags))]
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
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
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for len(out) < n {
		role := AllRoles[rng.IntN(len(AllRoles))]
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
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
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for len(out) < n {
		v := values[rng.IntN(len(values))]
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
