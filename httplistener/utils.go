package httplistener

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
)

func jsonError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	data := fmt.Sprintf("{\"error\":%q}\n", message)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.WriteHeader(code)
	w.Write([]byte(data))
}

var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

func getBuf() *bytes.Buffer {
	if bb, ok := bufPool.Get().(*bytes.Buffer); ok {
		return bb
	}
	return new(bytes.Buffer)
}

func putBuf(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}

// A Body is a ReadCloser with a static []byte message inside.
type Body struct {
	buf []byte
	off int
}

func newBody(b []byte) *Body { return &Body{b, 0} }

func (b *Body) dup() *Body { return &Body{b.buf, 0} }

func (b *Body) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n := copy(p, b.buf[b.off:])
	if n == 0 {
		return 0, io.EOF
	}
	b.off += n
	return n, nil
}

func (b *Body) Close() error {
	b.off = len(b.buf)
	return nil
}
