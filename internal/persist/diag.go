package persist

import (
	"fmt"
	"runtime/debug"
)

type loadDiag struct {
	file         string
	dbPath       string
	bucket       string
	size         int64
	version      byte
	versionKnown bool
}

func (d loadDiag) context() string {
	version := "unknown"
	if d.versionKnown {
		version = fmt.Sprintf("%d", d.version)
	}
	if d.size >= 0 {
		return fmt.Sprintf("persisted index file=%q db=%q bucket=%q size=%d version=%s", d.file, d.dbPath, d.bucket, d.size, version)
	}
	return fmt.Sprintf("persisted index file=%q db=%q bucket=%q version=%s", d.file, d.dbPath, d.bucket, version)
}

func (d loadDiag) wrap(stage string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s stage=%s: %w", d.context(), stage, err)
}

func recoverLoad(err *error, diag *loadDiag) {
	if r := recover(); r != nil {
		panicErr := fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		if diag != nil {
			*err = diag.wrap("panic", panicErr)
			return
		}
		*err = panicErr
	}
}
