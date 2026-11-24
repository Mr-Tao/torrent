//go:build linux && arm && cgo

package piecesha1

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stddef.h>

typedef struct evp_md_ctx_st EVP_MD_CTX;
typedef struct evp_md_st EVP_MD;
typedef struct engine_st ENGINE;

static EVP_MD_CTX *(*p_new)(void);
static void (*p_free)(EVP_MD_CTX *);
static const EVP_MD *(*p_sha1)(void);
static int (*p_init)(EVP_MD_CTX *, const EVP_MD *, ENGINE *);
static int (*p_update)(EVP_MD_CTX *, const void *, size_t);
static int (*p_final)(EVP_MD_CTX *, unsigned char *, unsigned int *);
static int (*p_copy)(EVP_MD_CTX *, const EVP_MD_CTX *);

static const char *openssl_load(void) {
	void *h = dlopen("libcrypto.so.1.1", RTLD_NOW | RTLD_LOCAL);
	if (!h)
		h = dlopen("libcrypto.so.3", RTLD_NOW | RTLD_LOCAL);
	if (!h)
		return dlerror();

	p_new = (EVP_MD_CTX *(*)(void))dlsym(h, "EVP_MD_CTX_new");
	p_free = (void (*)(EVP_MD_CTX *))dlsym(h, "EVP_MD_CTX_free");
	p_sha1 = (const EVP_MD *(*)(void))dlsym(h, "EVP_sha1");
	p_init = (int (*)(EVP_MD_CTX *, const EVP_MD *, ENGINE *))
		dlsym(h, "EVP_DigestInit_ex");
	p_update = (int (*)(EVP_MD_CTX *, const void *, size_t))
		dlsym(h, "EVP_DigestUpdate");
	p_final = (int (*)(EVP_MD_CTX *, unsigned char *, unsigned int *))
		dlsym(h, "EVP_DigestFinal_ex");
	p_copy = (int (*)(EVP_MD_CTX *, const EVP_MD_CTX *))
		dlsym(h, "EVP_MD_CTX_copy_ex");
	if (!p_new || !p_free || !p_sha1 || !p_init ||
		!p_update || !p_final || !p_copy)
		return "missing OpenSSL EVP symbol";
	// Keep the library loaded: the function pointers live here.
	return NULL;
}

static void *openssl_new(void) {
	return p_new();
}
static void openssl_free(void *ctx) {
	p_free((EVP_MD_CTX *)ctx);
}
static int openssl_begin(void *ctx) {
	return p_init((EVP_MD_CTX *)ctx, p_sha1(), NULL);
}
static int openssl_write(void *ctx, const void *buf, size_t n) {
	return p_update((EVP_MD_CTX *)ctx, buf, n);
}
static int openssl_copy(void *dst, void *src) {
	return p_copy((EVP_MD_CTX *)dst, (EVP_MD_CTX *)src);
}
static int openssl_end(void *ctx, unsigned char *out) {
	unsigned int n = 0;
	return p_final((EVP_MD_CTX *)ctx, out, &n) == 1 && n == 20;
}
*/
import "C"

import (
	"crypto/sha1"
	"errors"
	"hash"
	"runtime"
	"sync"
	"unsafe"
)

var (
	loadOnce sync.Once
	loadErr  string
)

type digest struct {
	ctx unsafe.Pointer
}

var _ hash.Hash = (*digest)(nil)

func newOpenSSL() hash.Hash {
	loadOnce.Do(func() {
		if msg := C.openssl_load(); msg != nil {
			loadErr = C.GoString(msg)
		}
	})
	if loadErr != "" {
		panic("piecesha1: OpenSSL unavailable: " + loadErr)
	}
	ctx := C.openssl_new()
	if ctx == nil {
		panic("piecesha1: EVP_MD_CTX_new failed")
	}
	d := &digest{ctx: ctx}
	d.Reset()
	// The verification path explicitly calls Close. This is a safeguard
	// against callers that accidentally forget to release a native context.
	runtime.SetFinalizer(d, (*digest).Close)
	return d
}

func (d *digest) Reset() {
	if d.ctx == nil || C.openssl_begin(d.ctx) != 1 {
		panic("piecesha1: EVP_DigestInit_ex failed")
	}
	runtime.KeepAlive(d)
}

func (d *digest) Write(p []byte) (int, error) {
	if d.ctx == nil {
		return 0, errors.New("piecesha1: use after Close")
	}
	if len(p) == 0 {
		return 0, nil
	}
	ok := C.openssl_write(d.ctx, unsafe.Pointer(&p[0]), C.size_t(len(p)))
	runtime.KeepAlive(d)
	if ok != 1 {
		return 0, errors.New("piecesha1: EVP_DigestUpdate failed")
	}
	return len(p), nil
}

// Sum must leave d unchanged. OpenSSL Final consumes its context, so
// calculate the digest from a temporary EVP_MD_CTX_copy_ex copy.
func (d *digest) Sum(b []byte) []byte {
	if d.ctx == nil {
		panic("piecesha1: Sum after Close")
	}
	copyCtx := C.openssl_new()
	if copyCtx == nil {
		panic("piecesha1: EVP_MD_CTX_new for Sum failed")
	}
	defer C.openssl_free(copyCtx)
	if C.openssl_copy(copyCtx, d.ctx) != 1 {
		panic("piecesha1: EVP_MD_CTX_copy_ex failed")
	}
	var sum [sha1.Size]byte
	if C.openssl_end(copyCtx, (*C.uchar)(unsafe.Pointer(&sum[0]))) != 1 {
		panic("piecesha1: EVP_DigestFinal_ex failed")
	}
	runtime.KeepAlive(d)
	return append(b, sum[:]...)
}

func (*digest) Size() int      { return sha1.Size }
func (*digest) BlockSize() int { return sha1.BlockSize }

func (d *digest) Close() {
	if d.ctx == nil {
		return
	}
	runtime.SetFinalizer(d, nil)
	ctx := d.ctx
	d.ctx = nil
	C.openssl_free(ctx)
}
