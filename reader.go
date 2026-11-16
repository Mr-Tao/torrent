package torrent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/anacrolix/log"
	"github.com/anacrolix/missinggo/v2"
)

// Accesses Torrent data via a Client. Reads block until the data is available. Seeks and readahead
// also drive Client behaviour.
type Reader interface {
	io.Reader
	io.Seeker
	io.Closer
	missinggo.ReadContexter
	// Configure the number of bytes ahead of a read that should also be prioritized in preparation
	// for further reads.
	SetReadahead(int64)
	// Don't wait for pieces to complete and be verified. Read calls return as soon as they can when
	// the underlying chunks become available.
	SetResponsive()
}

// Piece range by piece index, [begin, end).
type pieceRange struct {
	begin, end pieceIndex
}

type readerReadEpoch struct {
	permit      chan struct{}
	invalidated chan struct{}
}

func newReaderReadEpoch() *readerReadEpoch {
	e := &readerReadEpoch{
		permit:      make(chan struct{}, 1),
		invalidated: make(chan struct{}),
	}
	e.permit <- struct{}{}
	return e
}

type reader struct {
	t          *Torrent
	responsive bool
	// Adjust the read/seek window to handle Readers locked to File extents and the like.
	offset, length int64

	// Required when modifying pos and readahead, or reading them without opMu.
	mu  sync.Locker
	pos int64
	// Incremented by every successful Seek.
	// Protected by mu (the torrent client's lock).
	posGeneration uint64
	readahead     int64
	// The cached piece range this reader wants downloaded. The zero value corresponds to nothing.
	// We cache this so that changes can be detected, and bubbled up to the Torrent only as
	// required.
	pieces pieceRange
	// Protected by mu. Reads within one epoch are serialized,
	// but a Seek creates an independent epoch.
	readEpoch *readerReadEpoch
}

var _ io.ReadCloser = (*reader)(nil)
var errReaderPositionChanged = errors.New(
	"torrent: reader position changed during read",
)

func (r *reader) SetResponsive() {
	r.responsive = true
	r.t.cl.event.Broadcast()
}

// Disable responsive mode. TODO: Remove?
func (r *reader) SetNonResponsive() {
	r.responsive = false
	r.t.cl.event.Broadcast()
}

func (r *reader) SetReadahead(readahead int64) {
	r.mu.Lock()
	r.readahead = readahead
	r.mu.Unlock()
	r.t.cl.lock()
	defer r.t.cl.unlock()
	r.posChanged()
}

// How many bytes are available to read. Max is the most we could require.
func (r *reader) available(off, max int64) (ret int64) {
	off += r.offset
	for max > 0 {
		req, ok := r.t.offsetRequest(off)
		if !ok {
			break
		}
		if !r.responsive && !r.t.pieceComplete(pieceIndex(req.Index)) {
			break
		}
		if !r.t.haveChunk(req) {
			break
		}
		len1 := int64(req.Length) - (off - r.t.requestOffset(req))
		max -= len1
		ret += len1
		off += len1
	}
	// Ensure that ret hasn't exceeded our original max.
	if max < 0 {
		ret += max
	}
	return
}

func (r *reader) waitReadable(off int64) {
	// We may have been sent back here because we were told we could read but it failed.
	r.t.cl.event.Wait()
}

// Calculates the pieces this reader wants downloaded, ignoring the cached value at r.pieces.
func (r *reader) piecesUncached() (ret pieceRange) {
	ra := r.readahead
	if ra < 1 {
		// Needs to be at least 1, because [x, x) means we don't want
		// anything.
		ra = 1
	}
	if ra > r.length-r.pos {
		ra = r.length - r.pos
	}
	ret.begin, ret.end = r.t.byteRegionPieces(r.torrentOffset(r.pos), ra)
	return
}

func (r *reader) Read(b []byte) (n int, err error) {
	return r.ReadContext(context.Background(), b)
}

func (r *reader) ReadContext(ctx context.Context, b []byte) (n int, err error) {
	var ctxErr error

	if ctx.Done() != nil {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			<-ctx.Done()

			r.t.cl.lock()
			ctxErr = ctx.Err()
			r.t.tickleReaders()
			r.t.cl.unlock()
		}()
	}

	// Select the current read epoch.
	r.mu.Lock()
	if r.readEpoch == nil {
		r.readEpoch = newReaderReadEpoch()
	}
	epoch := r.readEpoch
	r.mu.Unlock()

	// Serialize reads only within this epoch.
	// Waiting can be interrupted by a seek or cancellation.
	select {
	case <-ctx.Done():
		return 0, ctx.Err()

	case <-epoch.invalidated:
		return 0, errReaderPositionChanged

	case <-epoch.permit:
	}
	defer func() {
		epoch.permit <- struct{}{}
	}()

	// A Seek could have happened while waiting for the permit.
	r.mu.Lock()
	if r.readEpoch != epoch {
		r.mu.Unlock()
		return 0, errReaderPositionChanged
	}

	startPos := r.pos
	startGeneration := r.posGeneration
	r.mu.Unlock()

	n, err = r.readOnceAt(
		b,
		startPos,
		startGeneration,
		&ctxErr,
	)

	if n == 0 {
		if err == nil && len(b) > 0 {
			panic("expected error")
		}
		return
	}

	r.mu.Lock()

	if r.posGeneration == startGeneration {
		r.pos += int64(n)
		r.posChanged()
	}

	r.mu.Unlock()

	if startPos+int64(n) >= r.length {
		err = io.EOF
	} else if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}

	return
}

// Wait until some data should be available to read. Tickles the client if it isn't. Returns how
// much should be readable without blocking.
func (r *reader) waitAvailable(
	pos, wanted int64,
	generation uint64,
	ctxErr *error,
	wait bool,
) (avail int64, err error) {
	r.t.cl.lock()
	defer r.t.cl.unlock()

	for {
		// Seek invalidated this read while it was waiting.
		if r.posGeneration != generation {
			return 0, errReaderPositionChanged
		}

		avail = r.available(pos, wanted)
		if avail != 0 {
			return
		}

		if r.t.closed.IsSet() {
			return 0, errors.New("torrent closed")
		}

		if *ctxErr != nil {
			return 0, *ctxErr
		}

		if !r.t.networkingEnabled {
			return 0, errors.New("data not already available")
		}

		if !wait || wanted == 0 {
			return
		}

		r.waitReadable(pos)
	}
}

// Adds the reader's torrent offset to the reader object offset (for example the reader might be
// constrainted to a particular file within the torrent).
func (r *reader) torrentOffset(readerPos int64) int64 {
	return r.offset + readerPos
}

// Performs at most one successful read to torrent storage.
func (r *reader) readOnceAt(b []byte, pos int64, generation uint64, ctxErr *error) (n int, err error) {
	if pos >= r.length {
		err = io.EOF
		return
	}
	for {
		var avail int64
		avail, err = r.waitAvailable(
			pos,
			int64(len(b)),
			generation,
			ctxErr,
			n == 0,
		)
		if avail == 0 {
			return
		}
		firstPieceIndex := pieceIndex(r.torrentOffset(pos) / r.t.info.PieceLength)
		firstPieceOffset := r.torrentOffset(pos) % r.t.info.PieceLength
		b1 := missinggo.LimitLen(b, avail)
		n, err = r.t.readAt(b1, r.torrentOffset(pos))
		if n != 0 {
			err = nil
			return
		}

		r.t.cl.lock()

		// A seek may have moved the reader to a different piece
		// while storage I/O was in progress.
		//
		// The old read must not inspect or modify piece state
		// based on the new reader position.
		if r.posGeneration != generation {
			r.t.cl.unlock()
			return 0, errReaderPositionChanged
		}

		// TODO: Just reset pieces in the readahead window. This might help
		// prevent thrashing with small caches and file and piece priorities.
		r.log(log.Fstr("error reading torrent %s piece %d offset %d, %d bytes: %v",
			r.t.infoHash.HexString(), firstPieceIndex, firstPieceOffset, len(b1), err))
		if !r.t.updatePieceCompletion(firstPieceIndex) {
			r.log(log.Fstr("piece %d completion unchanged", firstPieceIndex))
		}
		// Update the rest of the piece completions in the readahead window, without alerting to
		// changes (since only the first piece, the one above, could have generated the read error
		// we're currently handling).
		if r.pieces.begin != firstPieceIndex {
			panic(fmt.Sprint(r.pieces.begin, firstPieceIndex))
		}
		for index := r.pieces.begin + 1; index < r.pieces.end; index++ {
			r.t.updatePieceCompletion(index)
		}
		r.t.cl.unlock()
	}
}

// Hodor
func (r *reader) Close() error {
	r.t.cl.lock()
	defer r.t.cl.unlock()
	r.t.deleteReader(r)
	return nil
}

func (r *reader) posChanged() {
	to := r.piecesUncached()
	from := r.pieces

	if to == from {
		if r.responsive {
			// The read window moved inside the same piece.
			// Request more chunks near the new position.
			for c := range r.t.conns {
				c.updateRequests()
			}
		}
		return
	}

	r.pieces = to
	// log.Printf("reader pos changed %v->%v", from, to)
	r.t.readerPosChanged(from, to)
}

func (r *reader) Seek(off int64, whence int) (ret int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch whence {
	case io.SeekStart:
		r.pos = off
	case io.SeekCurrent:
		r.pos += off
	case io.SeekEnd:
		r.pos = r.length + off
	default:
		return r.pos, errors.New("bad whence")
	}

	// Invalidate position updates from old reads.
	r.posGeneration++

	// Release reads waiting in the old epoch.
	// Already running storage I/O may continue, but reads
	// started after this Seek use a fresh permit.
	if r.readEpoch != nil {
		close(r.readEpoch.invalidated)
	}
	r.readEpoch = newReaderReadEpoch()

	ret = r.pos
	r.posChanged()
	// Wake reads waiting for data at the old position.
	// They will detect the changed generation and exit.
	r.t.tickleReaders()
	return
}

func (r *reader) log(m log.Msg) {
	r.t.logger.LogLevel(log.Debug, m.Skip(1))
}
