package main

import (
	"bufio"
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

type lineReader struct {
	lines   chan string
	updates chan struct{}

	mu      sync.Mutex
	buffer  []byte
	raw     bool
	fd      int
	orig    *unix.Termios
	closeMu sync.Once
}

func newLineReader() (*lineReader, error) {
	lr := &lineReader{
		lines:   make(chan string, 8),
		updates: make(chan struct{}, 1),
		fd:      int(os.Stdin.Fd()),
	}
	if isTerminal(lr.fd) {
		orig, err := unix.IoctlGetTermios(lr.fd, unix.TCGETS)
		if err == nil {
			raw := *orig
			raw.Lflag &^= unix.ICANON | unix.ECHO
			raw.Cc[unix.VMIN] = 1
			raw.Cc[unix.VTIME] = 0
			if err := unix.IoctlSetTermios(lr.fd, unix.TCSETS, &raw); err == nil {
				lr.raw = true
				lr.orig = orig
				go lr.readRaw()
				return lr, nil
			}
		}
	}
	go lr.readScanner()
	return lr, nil
}

func (lr *lineReader) Lines() <-chan string {
	return lr.lines
}

func (lr *lineReader) Updates() <-chan struct{} {
	return lr.updates
}

func (lr *lineReader) Interactive() bool {
	return lr.raw
}

func (lr *lineReader) Buffer() string {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	return string(lr.buffer)
}

func (lr *lineReader) Close() error {
	var err error
	lr.closeMu.Do(func() {
		if lr.raw && lr.orig != nil {
			err = unix.IoctlSetTermios(lr.fd, unix.TCSETS, lr.orig)
		}
	})
	return err
}

func (lr *lineReader) readRaw() {
	defer close(lr.lines)
	buf := make([]byte, 1)
	for {
		n, err := os.Stdin.Read(buf)
		if err != nil {
			if err != io.EOF {
				return
			}
			return
		}
		if n == 0 {
			continue
		}
		switch ch := buf[0]; ch {
		case '\r', '\n':
			line := lr.drainBuffer()
			lr.signalUpdate()
			lr.lines <- line
		case 0x04:
			return
		case 0x7f, 0x08:
			lr.backspace()
			lr.signalUpdate()
		default:
			if ch >= 32 {
				lr.appendByte(ch)
				lr.signalUpdate()
			}
		}
	}
}

func (lr *lineReader) readScanner() {
	defer close(lr.lines)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		lr.signalUpdate()
		lr.lines <- scanner.Text()
	}
}

func (lr *lineReader) appendByte(ch byte) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	lr.buffer = append(lr.buffer, ch)
}

func (lr *lineReader) backspace() {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	if len(lr.buffer) > 0 {
		lr.buffer = lr.buffer[:len(lr.buffer)-1]
	}
}

func (lr *lineReader) drainBuffer() string {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	line := string(lr.buffer)
	lr.buffer = lr.buffer[:0]
	return line
}

func (lr *lineReader) signalUpdate() {
	select {
	case lr.updates <- struct{}{}:
	default:
	}
}

func isTerminal(fd int) bool {
	if _, err := unix.IoctlGetTermios(fd, unix.TCGETS); err == nil {
		return true
	}
	_, err := unix.IoctlGetWinsize(fd, unix.TIOCGWINSZ)
	return err == nil
}
