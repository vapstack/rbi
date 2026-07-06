package rbierrors

import (
	"errors"
)

var (
	ErrNotStructType              = errors.New("value is not a struct")
	ErrClosed                     = errors.New("collection closed")
	ErrBroken                     = errors.New("index is broken")
	ErrNoIndex                    = errors.New("index is disabled (transparent mode)")
	ErrInvalidQuery               = errors.New("invalid query")
	ErrInvalidBucketName          = errors.New("invalid bucket name")
	ErrUniqueViolation            = errors.New("unique constraint violation")
	ErrNilValue                   = errors.New("value is nil")
	ErrInvalidStringStorageFormat = errors.New("invalid string storage format")
	ErrPersistedIndexStale        = errors.New("persisted index is stale")
	ErrPersistedIndexInvalid      = errors.New("persisted index is invalid")
	ErrNilTx                      = errors.New("nil transaction")
	ErrStoreMismatch              = errors.New("transaction root store mismatch")
	ErrWrongTx                    = errors.New("wrong transaction type")
	ErrTxDone                     = errors.New("transaction is done")
	ErrCollectionNotVisible       = errors.New("collection is not visible in transaction")
	ErrGeneratedWriteDepth        = errors.New("generated write depth exceeded")
)
