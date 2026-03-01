package extbench

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
