package torrent

import (
	"errors"
	"fmt"

	"github.com/anacrolix/missinggo/bitmap"
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

	if writing || p.hashing || p.queuedForHash() {
		return 0, ErrChunkDiscardBusy
	}

	wasComplete := t.pieceComplete(p.index)

	// A complete piece represents all chunks through its
	// completion flag. Before discarding any bytes, clear
	// that flag in storage. The Client lock prevents other
	// torrent operations from observing the transition.
	if wasComplete {
		if err := p.Storage().MarkNotComplete(); err != nil {
			return 0, fmt.Errorf(
				"torrent: mark piece incomplete: %w", err,
			)
		}
	}

	// The callback must be all-or-nothing. If it fails,
	// restore the original completion state.
	if err := discard(); err != nil {
		if wasComplete {
			if restoreErr := p.Storage().MarkComplete(); restoreErr != nil {
				return 0, fmt.Errorf(
					"torrent: discard failed: %v; "+
						"restore completion failed: %w",
					err, restoreErr,
				)
			}
		}
		return 0, err
	}

	if wasComplete {
		// Completion previously represented the availability
		// of every chunk. Reconstruct that state explicitly,
		// then invalidate only the discarded range.
		p.dirtyChunks.AddRange(
			0,
			bitmap.BitIndex(p.numChunks()),
		)
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

	if wasComplete {
		// This also updates priorities and notifies readers
		// about the transition from complete to incomplete.
		t.updatePieceCompletion(p.index)
	}

	if invalidated != 0 {
		t.tickleReaders()

		for c := range t.conns {
			c.updateRequests()
		}

		if t.pieceStateChanges != nil {
			t.publishPieceChange(p.index)
		}
	}

	return invalidated, nil
}

// DiscardAllChunks atomically discards all stored data for a piece
// and invalidates its received-chunk bitmap.
//
// Unlike DiscardChunkRange, this also supports completed pieces.
// The callback must be synchronous, all-or-nothing, and must not
// call back into the torrent client.
func (p *Piece) DiscardAllChunks(discard func() error) error {
	if p == nil || p.t == nil || p.t.cl == nil {
		return errors.New("torrent: invalid piece")
	}
	if discard == nil {
		return errors.New("torrent: nil discard callback")
	}

	t := p.t
	t.cl.lock()
	defer t.cl.unlock()

	if !t.haveInfo() ||
		p.index < 0 ||
		p.index >= t.numPieces() ||
		t.closed.IsSet() {
		return errors.New("torrent: piece not available")
	}

	p.pendingWritesMutex.Lock()
	writing := p.pendingWrites != 0
	p.pendingWritesMutex.Unlock()

	if writing || p.hashing || p.queuedForHash() {
		return ErrChunkDiscardBusy
	}

	// The callback must clear storage completion as well as bytes.
	// The client lock prevents new received chunks from being
	// registered while the operation is in progress.
	if err := discard(); err != nil {
		return err
	}

	p.dirtyChunks.Clear()

	// Refresh completedPieces and scheduling from storage.
	changed := t.updatePieceCompletion(p.index)
	if !changed {
		t.updatePiecePriority(p.index)
	}

	// Even if completion did not change, chunk availability did.
	t.tickleReaders()
	for c := range t.conns {
		c.updateRequests()
	}

	if t.pieceStateChanges != nil {
		t.publishPieceChange(p.index)
	}

	return nil
}
