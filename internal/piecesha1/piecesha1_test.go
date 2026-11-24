package piecesha1

import (
	"bytes"
	"crypto/sha1"
	"hash"
	"testing"
)

func verifyDigest(t *testing.T, h hash.Hash, size int) {
	t.Helper()
	defer Close(h)

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*73 + 17)
	}
	for off := 0; off < len(data); {
		end := off + 1 + off%1009
		if end > len(data) {
			end = len(data)
		}
		n, err := h.Write(data[off:end])
		if err != nil || n != end-off {
			t.Fatalf("Write: n=%d, err=%v", n, err)
		}
		off = end
	}
	if n, err := h.Write(nil); n != 0 || err != nil {
		t.Fatalf("empty Write: %d, %v", n, err)
	}

	want := sha1.Sum(data)
	first := h.Sum(nil)
	if !bytes.Equal(first, want[:]) {
		t.Fatalf("size %d: first Sum got %x, want %x", size, first, want)
	}
	if second := h.Sum(nil); !bytes.Equal(second, want[:]) {
		t.Fatalf("size %d: repeated Sum changed the digest", size)
	}
	prefix := []byte{0x44, 0x55}
	withPrefix := h.Sum(append([]byte(nil), prefix...))
	if !bytes.Equal(withPrefix[:2], prefix) || !bytes.Equal(withPrefix[2:], want[:]) {
		t.Fatalf("size %d: Sum(prefix) is incorrect", size)
	}

	tail := []byte("continued streaming")
	if _, err := h.Write(tail); err != nil {
		t.Fatal(err)
	}
	continued := append(append([]byte(nil), data...), tail...)
	wantContinued := sha1.Sum(continued)
	if got := h.Sum(nil); !bytes.Equal(got, wantContinued[:]) {
		t.Fatalf("size %d: Sum mutated the streaming state", size)
	}

	h.Reset()
	if _, err := h.Write(tail); err != nil {
		t.Fatal(err)
	}
	wantReset := sha1.Sum(tail)
	if got := h.Sum(nil); !bytes.Equal(got, wantReset[:]) {
		t.Fatalf("size %d: Reset failed", size)
	}
	if h.Size() != sha1.Size || h.BlockSize() != sha1.BlockSize {
		t.Fatalf("Size/BlockSize mismatch")
	}
}

func TestGoFallback(t *testing.T) {
	t.Setenv("TS_SHA1_OPENSSL", "0")
	for _, size := range []int{0, 1, 63, 64, 65, 16 << 10, 2 << 20} {
		verifyDigest(t, New(), size)
	}
}
