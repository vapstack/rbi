package pools

const (
	minNumericShift = 5
	maxNumericShift = 20

	MinNumericPooledCap = 1 << minNumericShift
	MaxNumericPooledCap = 1 << maxNumericShift

	// MaxNumericRetainedCap is the largest external capacity that may be
	// demoted into the largest bucket. Larger slices are dropped to avoid
	// retaining very large backing arrays through a smaller bucket.
	MaxNumericRetainedCap = MaxNumericPooledCap + MaxNumericPooledCap/4

	maxBucketDistance = 3
)
