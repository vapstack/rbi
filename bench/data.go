package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/vapstack/rbi"
)

// UserBench represents a user account in the system.
// It includes varied data types to test different index capabilities.
type UserBench struct {
	ID         uint64   `db:"id"`
	Name       string   `db:"name"`
	Email      string   `db:"email" rbi:"unique"`
	Country    string   `db:"country"`
	Plan       string   `db:"plan"`   // e.g., "free", "pro", "enterprise"
	Status     string   `db:"status"` // e.g., "active", "suspended", "pending"
	Age        int      `db:"age"`
	Score      float64  `db:"score"`
	IsVerified bool     `db:"is_verified"` // High cardinality boolean
	CreatedAt  int64    `db:"created_at"`  // Unix timestamp for range queries
	LastLogin  int64    `db:"last_login"`  // Frequently updated field
	Tags       []string `db:"tags"`        // For HAS/HASANY queries
	Roles      []string `db:"roles"`       // Security roles
	Blob       []byte   `db:"-" rbi:"-"`   // Non-indexed payload
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
	rng := newRand(1)

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

func seedData(db *rbi.DB[uint64, UserBench], maxID *uint64, count int) {
	rng := newRand(time.Now().UnixNano())
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
	r := rng.Float64()
	switch {
	case r < 0.91:
		return []string{"member"}
	case r < 0.96:
		return []string{"member", "trusted"}
	case r < 0.989:
		return []string{"member", "moderator"}
	case r < 0.997:
		return []string{"member", "admin"}
	case r < 0.999:
		return []string{"staff"}
	default:
		return []string{"bot"}
	}
}

func pickRandomSlice(rng *rand.Rand, src []string, max int) []string {
	if max <= 0 || len(src) == 0 {
		return nil
	}
	n := rng.IntN(max + 1)
	if n == 0 {
		return nil
	}
	if n > len(src) {
		n = len(src)
	}
	out := make([]string, 0, n)
	perm := rng.Perm(len(src))
	for i := 0; i < n; i++ {
		out = append(out, src[perm[i]])
	}
	return out
}
