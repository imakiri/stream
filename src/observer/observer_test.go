package observer

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testSrc struct {
	buf *bytes.Buffer
}

func (t testSrc) Read(p []byte) (n int, err error) {
	return t.buf.Read(p)
}

func (t testSrc) Close() error {
	t.buf.Reset()
	return nil
}

// testReporter is called from the reader's goroutine and read by the test.
type testReporter struct {
	mu   sync.Mutex
	data [][2]uint64
}

func newTestReporter() *testReporter {
	return &testReporter{
		data: make([][2]uint64, 0, 10),
	}
}

func (t *testReporter) Report(total, delta uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.data = append(t.data, [2]uint64{total, delta})
}

func (t *testReporter) reports() [][2]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([][2]uint64(nil), t.data...)
}

func TestReader(t *testing.T) {
	var data = []byte("qwerty12345")
	var src = testSrc{
		buf: bytes.NewBuffer(data),
	}

	var rep = newTestReporter()
	var reader, err = NewReader(src, rep, time.Second)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)
	t.Log(rep.reports())

	var buf0 = [][2]uint64{{0, 0}}
	require.Equal(t, buf0, rep.reports())

	var buf = make([]byte, 3)
	var n int

	n, err = reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	time.Sleep(time.Second)
	t.Log(rep.reports())

	var buf1 = [][2]uint64{{0, 0}, {3, 3}}
	require.Equal(t, buf1, rep.reports())

	n, err = reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	time.Sleep(time.Second)
	t.Log(rep.reports())

	var buf2 = [][2]uint64{{0, 0}, {3, 3}, {6, 3}}
	require.Equal(t, buf2, rep.reports())
}
