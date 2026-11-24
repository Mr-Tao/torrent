//go:build !linux || !arm || !cgo

package piecesha1

import "hash"

// An opted-in OpenSSL backend must never silently fall back to Go.
func newOpenSSL() hash.Hash {
	panic("piecesha1: TS_SHA1_OPENSSL=1 requires linux/arm and CGO_ENABLED=1")
}
