package wexec

import (
	"errors"
	"fmt"
)

var (
	ErrBucketMissing            = errors.New("bucket does not exist")
	ErrStringMapBucketMissing   = errors.New("string map bucket does not exist")
	ErrAdvanceBucketSequence    = errors.New("advance bucket sequence")
	ErrAdvanceStringMapSequence = errors.New("advance string map sequence")
)

type prepareErr uint8

const (
	prepareErrDecode prepareErr = iota + 1
	prepareErrDecodePreparedValue
	prepareErrDecodeExistingValue
	prepareErrRedecodeEmpty
	prepareErrRedecodeValue
	prepareErrApplyPatch
	prepareErrEncode
)

func formatPrepareErr(kind prepareErr, err error) error {
	switch kind {
	case prepareErrDecode:
		return fmt.Errorf("decode: %w", err)
	case prepareErrDecodePreparedValue:
		return fmt.Errorf("decode prepared value: %w", err)
	case prepareErrDecodeExistingValue:
		return fmt.Errorf("failed to decode existing value: %w", err)
	case prepareErrRedecodeEmpty:
		return errors.New("failed to re-decode value for patching: source payload is empty")
	case prepareErrRedecodeValue:
		return fmt.Errorf("failed to re-decode value for patching: %w", err)
	case prepareErrApplyPatch:
		return fmt.Errorf("failed to apply patch: %w", err)
	case prepareErrEncode:
		return fmt.Errorf("encode: %w", err)
	default:
		return err
	}
}

func formatBoltWriteErr(err error, op Op, id string, idx uint64, key, payload []byte) error {
	switch {

	case errors.Is(err, errEmptyPayload):
		return fmt.Errorf(
			"invalid write payload op=%s id=%s idx=%d key_len=%d payload_len=0: empty msgpack payload",
			op.String(),
			id,
			idx,
			len(key),
		)

	case op == opSet || op == opPatch:
		return fmt.Errorf(
			"put op=%s id=%s idx=%d key_len=%d payload_len=%d payload_prefix_hex=%s: %w",
			op.String(),
			id,
			idx,
			len(key),
			len(payload),
			formatBytesPrefix(payload, 32),
			err,
		)

	case op == opDelete:
		return fmt.Errorf(
			"delete op=%s id=%s idx=%d key_len=%d: %w",
			op.String(),
			id,
			idx,
			len(key),
			err,
		)

	default:
		return fmt.Errorf("unknown write op: %v", op)
	}
}

func formatBytesPrefix(b []byte, limit int) string {
	if len(b) == 0 {
		return "empty"
	}
	if limit <= 0 || len(b) <= limit {
		return fmt.Sprintf("%x", b)
	}
	return fmt.Sprintf("%x...(len=%d)", b[:limit], len(b))
}
