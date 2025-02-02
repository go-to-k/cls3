package io

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

type Writer struct {
	out       io.Writer
	mtx       sync.Mutex
	lineCount int
}

func New() *Writer {
	return &Writer{
		out: os.Stdout,
	}
}

func (w *Writer) Write(p []byte) (n int, err error) {
	w.mtx.Lock()
	defer w.mtx.Unlock()

	if w.lineCount > 0 {
		w.clearLines()
	}

	w.lineCount = strings.Count(string(p), "\n")
	if len(p) > 0 && !strings.HasSuffix(string(p), "\n") {
		w.lineCount++
	}

	return w.out.Write(p)
}

func (w *Writer) clearLines() {
	for i := 0; i < w.lineCount; i++ {
		fmt.Fprint(w.out, "\033[1A\033[2K")
	}
}

func (w *Writer) Flush() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	_, err := fmt.Fprintln(w.out)
	return err
}

func (w *Writer) Start() {}

func (w *Writer) Stop() {}
