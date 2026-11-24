# Experimental OpenSSL SHA-1 backend for verified pieces (ARMv7)

The default backend is Go's `crypto/sha1`. Set `TS_SHA1_OPENSSL=1` to
select the OpenSSL EVP backend for **piece verification only**. Other SHA-1
uses in the torrent library are unchanged.

Supported accelerated build: Linux/ARM32 with `CGO_ENABLED=1`.
`libcrypto.so.1.1` (or `libcrypto.so.3`) is loaded with `dlopen` when the
option is first used; OpenSSL development headers and link-time `libcrypto`
are not needed. On unsupported builds an explicit panic is raised if
the option is enabled, rather than silently falling back.

The `hash.Hash` adapter implements non-destructive `Sum` using
`EVP_MD_CTX_copy_ex`, and verification calls `piecesha1.Close` via
`defer` to release the native context. A Go finalizer is only a
defensive fallback for omitted closes.

To cross-compile the focused test using the WD armhf toolchain:

```sh
CGO_ENABLED=1 GOOS=linux GOARCH=arm GOARM=7 \
  CC=arm-linux-gnueabihf-gcc \
  go test -c -o piecesha1-arm.test ./internal/piecesha1
```

Run on the NAS with the native OpenSSL library installed:

```sh
TS_SHA1_OPENSSL=1 ./piecesha1-arm.test -test.v
```

Build TorrServer with CGo enabled, using the existing local module
`replace` for the torrent fork. Compare the same binary with
`TS_SHA1_OPENSSL=0` (default) and `TS_SHA1_OPENSSL=1`, restarting
TorrServer between runs. This has **not** yet been validated on the WD
device; do not replace a working production binary before testing.
