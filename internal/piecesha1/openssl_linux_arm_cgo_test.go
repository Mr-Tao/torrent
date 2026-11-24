//go:build linux && arm && cgo

package piecesha1

import "testing"

func TestOpenSSLEVP(t *testing.T) {
	t.Setenv("TS_SHA1_OPENSSL", "1")
	for _, size := range []int{0, 1, 63, 64, 65, 16 << 10, 64 << 10, 2 << 20, 16 << 20} {
		verifyDigest(t, New(), size)
	}
}
