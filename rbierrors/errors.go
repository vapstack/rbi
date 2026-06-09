package rbierrors

import (
	"errors"
)

var (
	ErrNotStructType              = errors.New("value is not a struct")
	ErrClosed                     = errors.New("database closed")
	ErrBroken                     = errors.New("index is broken")
	ErrNoIndex                    = errors.New("index is disabled (transparent mode)")
	ErrInvalidQuery               = errors.New("invalid query")
	ErrInvalidBucketName          = errors.New("invalid bucket name")
	ErrUniqueViolation            = errors.New("unique constraint violation")
	ErrNilValue                   = errors.New("value is nil")
	ErrInvalidStringStorageFormat = errors.New("invalid string storage format")
	ErrPersistedIndexStale        = errors.New("persisted index is stale")
	ErrPersistedIndexInvalid      = errors.New("persisted index is invalid")
	ErrCloneNil                   = errors.New("clone returned nil")
)
