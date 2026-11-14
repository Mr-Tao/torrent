package torrent

import (
	"errors"
	"fmt"
)

var ErrChunkDiscardBusy = errors.New(
	"torrent: piece has active writes or verification",
)

// DiscardChunkRange invalidates received chunks in a piece after
// discard has removed their data from storage.
//
// begin and length are relative to the piece and must cover whole
// torrent chunks, except that the final chunk may be shorter.
//
// discard runs synchronously under the Client lock. It must be
// quick, all-or-nothing, and must not call back into Torrent.
//
// This initial implementation only supports incomplete pieces
// that are not being written or hashed.
func (p *Piece) DiscardChunkRange(
	begin, length int64,
	discard func() error,
) (int, error) {
	if p == nil || p.t == nil || p.t.cl == nil {
		return 0, errors.New("torrent: invalid piece")
	}
	if discard == nil {
		return 0, errors.New("torrent: nil discard callback")
	}

	t := p.t
	t.cl.lock()
	defer t.cl.unlock()

	if !t.haveInfo() ||
		p.index < 0 || p.index >= t.numPieces() {
		return 0, errors.New("torrent: piece not available")
	}

	pieceLength := int64(p.length())
	chunkSize := int64(t.chunkSize)

	if chunkSize <= 0 ||
		begin < 0 || length <= 0 ||
		begin >= pieceLength ||
		length > pieceLength-begin {
		return 0, fmt.Errorf(
			"torrent: invalid discard range %d+%d",
			begin, length,
		)
	}

	end := begin + length
	if begin%chunkSize != 0 ||
		(end%chunkSize != 0 && end != pieceLength) {
		return 0, errors.New(
			"torrent: discard range is not chunk-aligned",
		)
	}

	p.pendingWritesMutex.Lock()
	writing := p.pendingWrites != 0
	p.pendingWritesMutex.Unlock()

	if writing ||
		p.hashing ||
		p.queuedForHash() ||
		t.pieceComplete(p.index) {
		return 0, ErrChunkDiscardBusy
	}

	// Discard the bytes before clearing the availability bits.
	// No new received chunk can be registered while we hold
	// the Client lock, and in-flight writes were rejected above.
	if err := discard(); err != nil {
		return 0, err
	}

	invalidated := 0
	first := int(begin / chunkSize)
	last := int((end + chunkSize - 1) / chunkSize)

	for i := first; i < last; i++ {
		if p.dirtyChunks.Contains(i) {
			p.pendChunkIndex(i)
			invalidated++
		}
	}

	if invalidated != 0 {
		t.tickleReaders()

		// Piece priority has not changed, so merely recalculating
		// its priority may not wake any peer writer.
		for c := range t.conns {
			c.updateRequests()
		}

		if t.pieceStateChanges != nil {
			t.publishPieceChange(p.index)
		}
	}

	return invalidated, nil
}
