package torrent

import (
	"errors"
	"testing"

	"github.com/anacrolix/torrent/metainfo"
	pp "github.com/anacrolix/torrent/peer_protocol"
)

func newDiscardTestTorrent() *Torrent {
	const size = 64 << 10

	info := &metainfo.Info{
		PieceLength: size,
		Length:      size,
		Pieces:      make([]byte, 20),
	}

	length := int64(size)
	cl := &Client{}
	t := &Torrent{
		cl:        cl,
		info:      info,
		length:    &length,
		chunkSize: pp.Integer(16 << 10),
	}

	t.pieces = []Piece{{
		t:     t,
		index: 0,
	}}
	return t
}

func TestDiscardChunkRange(t *testing.T) {
	tor := newDiscardTestTorrent()
	p := &tor.pieces[0]

	// Three received blocks, fourth still pending.
	for i := 0; i < 3; i++ {
		p.unpendChunkIndex(i)
	}

	req, ok := tor.offsetRequest(16 << 10)
	if !ok || !tor.haveChunk(req) {
		t.Fatal("block should initially be available")
	}

	discarded := false
	n, err := p.DiscardChunkRange(
		16<<10, 16<<10,
		func() error {
			discarded = true
			return nil
		},
	)

	if err != nil || n != 1 || !discarded {
		t.Fatalf(
			"discard: n=%d err=%v called=%v",
			n, err, discarded,
		)
	}

	if tor.haveChunk(req) {
		t.Fatal("discarded block is still reported available")
	}

	// The request iterator must offer the discarded block again.
	found := false
	iterUndirtiedChunks(0, tor, func(cs chunkSpec) bool {
		if int64(cs.Begin) == 16<<10 {
			found = true
		}
		return true
	})
	if !found {
		t.Fatal("discarded block was not requeued")
	}

	// Simulate receiving the same block again.
	p.unpendChunkIndex(1)
	if !tor.haveChunk(req) {
		t.Fatal("re-downloaded block is not available")
	}
}

func TestDiscardChunkRangeBusy(t *testing.T) {
	tor := newDiscardTestTorrent()
	p := &tor.pieces[0]
	p.unpendChunkIndex(0)

	// Simulate a write that has not yet completed.
	p.pendingWritesMutex.Lock()
	p.pendingWrites = 1
	p.pendingWritesMutex.Unlock()

	called := false
	_, err := p.DiscardChunkRange(
		0, 16<<10,
		func() error {
			called = true
			return nil
		},
	)

	if !errors.Is(err, ErrChunkDiscardBusy) {
		t.Fatalf("expected busy error, got %v", err)
	}
	if called {
		t.Fatal("discard was called during an active write")
	}

	p.pendingWritesMutex.Lock()
	p.pendingWrites = 0
	p.pendingWritesMutex.Unlock()

	if !p.dirtyChunks.Contains(0) {
		t.Fatal("busy discard invalidated a block")
	}
}

func TestDiscardChunkRangeRejectedDuringHash(t *testing.T) {
	tor := newDiscardTestTorrent()
	p := &tor.pieces[0]
	p.unpendChunkIndex(0)
	p.hashing = true

	_, err := p.DiscardChunkRange(
		0, 16<<10, func() error { return nil },
	)
	if !errors.Is(err, ErrChunkDiscardBusy) {
		t.Fatalf("expected busy error, got %v", err)
	}
}
