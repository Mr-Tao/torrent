// Package piecesha1 selects the SHA-1 backend used to verify torrent pieces.
// OpenSSL is opt-in; all other SHA-1 uses remain on the Go implementation.
package piecesha1

import (
	"crypto/sha1"
	"hash"
	"os"
)

// New returns a SHA-1 hasher. TS_SHA1_OPENSSL=1 selects OpenSSL on
// linux/arm builds with cgo enabled; by default crypto/sha1 is used.
// The caller must call Close when finished.
func New() hash.Hash {
	if os.Getenv("TS_SHA1_OPENSSL") == "1" {
		return newOpenSSL()
	}
	return sha1.New()
}

// Close releases backend-specific resources. It is a no-op for crypto/sha1.
// Unlike hash.Hash, an OpenSSL-backed hash owns a native EVP_MD_CTX.
func Close(h hash.Hash) {
	if c, ok := h.(interface{ Close() }); ok {
		c.Close()
	}
}
