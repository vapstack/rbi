package rbi

import (
	"io"
	"log"
)

var testDiscardLogger = log.New(io.Discard, "", 0)

func testOptions(opts Options) Options {
	if opts.Logger == nil {
		opts.Logger = testDiscardLogger
	}
	return opts
}
