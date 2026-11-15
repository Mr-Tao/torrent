package torrent

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// A fake storage backend that blocks a read until the
// test explicitly allows it to finish.
type seekTestStorage struct {
	entered       chan struct{}
	release       chan struct{}
	once          sync.Once
	failFirstRead atomic.Bool
}

func (s *seekTestStorage) OpenTorrent(
	*metainfo.Info, metainfo.Hash,
) (storage.TorrentImpl, error) {
	return s, nil
}

type seekTestPiece struct {
	*seekTestStorage
	base int64
}

func (s *seekTestStorage) Piece(
	m metainfo.Piece,
) storage.PieceImpl {
	return &seekTestPiece{
		seekTestStorage: s,
		base:            m.Offset(),
	}
}

func (p *seekTestPiece) ReadAt(
	b []byte, off int64,
) (int, error) {
	return p.seekTestStorage.ReadAt(b, p.base+off)
}

func (s *seekTestStorage) Close() error {
	return nil
}

func (s *seekTestStorage) Completion() storage.Completion {
	return storage.Completion{Complete: true, Ok: true}
}

func (s *seekTestStorage) MarkComplete() error {
	return nil
}

func (s *seekTestStorage) MarkNotComplete() error {
	return nil
}

func (s *seekTestStorage) WriteAt(
	b []byte, _ int64,
) (int, error) {
	return len(b), nil
}

func (s *seekTestStorage) ReadAt(
	b []byte, off int64,
) (int, error) {
	s.once.Do(func() {
		close(s.entered)
	})

	<-s.release
	if s.failFirstRead.Swap(false) {
		return 0, io.ErrUnexpectedEOF
	}

	for i := range b {
		b[i] = byte((off + int64(i)) % 251)
	}
	return len(b), nil
}

func TestReaderConcurrentSeekPreservesPosition(t *testing.T) {
	testReaderConcurrentSeek(t, false)
}

func TestReaderSeekDuringReadError(t *testing.T) {
	testReaderConcurrentSeek(t, true)
}

func testReaderConcurrentSeek(t *testing.T, failFirstRead bool) {
	store := &seekTestStorage{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	store.failFirstRead.Store(failFirstRead)

	// Ensure that a failed test never leaves ReadAt blocked.
	defer func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	}()

	info := metainfo.Info{
		Name:        "seek-test.bin",
		Length:      64 << 10,
		PieceLength: 16 << 10,
		Pieces:      make([]byte, 4*20),
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}

	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}

	cfg := NewDefaultClientConfig()
	cfg.ListenHost = func(string) string {
		return "127.0.0.1"
	}
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableUTP = true
	cfg.DisableIPv6 = true
	cfg.DisablePEX = true
	cfg.DisableTrackers = true
	cfg.NoDefaultPortForwarding = true
	cfg.DefaultStorage = store

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	torr, _, err := client.AddTorrentSpec(
		TorrentSpecFromMetaInfo(mi),
	)
	if err != nil {
		t.Fatal(err)
	}

	reader := torr.NewReader()
	defer reader.Close()

	type readResult struct {
		n          int
		err        error
		panicValue any
	}

	done := make(chan readResult, 1)

	go func() {
		buf := make([]byte, 1024)
		n, err := reader.Read(buf)
		done <- readResult{n: n, err: err}
	}()

	// Wait until Read() reaches the storage backend.
	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("read did not reach storage")
	}

	// Read is now blocked inside ReadAt().
	// Seek must still be able to change the position.
	const seekTarget int64 = 32 << 10

	pos, err := reader.Seek(seekTarget, io.SeekStart)
	if err != nil || pos != seekTarget {
		t.Fatalf("seek: pos=%d err=%v", pos, err)
	}

	// Allow the earlier read to finish.
	close(store.release)

	select {
	case result := <-done:
		if result.panicValue != nil {
			t.Fatalf("reader panicked after concurrent seek: %v",
				result.panicValue)
		}

		switch {
		case !failFirstRead && result.n == 1024 && result.err == nil:
			// Successful read.

		case failFirstRead &&
			result.n == 0 &&
			errors.Is(result.err, errReaderPositionChanged):
			// The read was invalidated by a concurrent seek.

		default:
			t.Fatalf(
				"unexpected read result: n=%d err=%v",
				result.n, result.err,
			)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("read did not finish")
	}

	// Completing an old read must not advance a newer seek.
	pos, err = reader.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}

	if pos != seekTarget {
		t.Fatalf(
			"read advanced position after seek: got=%d want=%d",
			pos, seekTarget,
		)
	}

	// Verify that the next read actually starts at the seek target.
	next := make([]byte, 1)

	n, err := reader.Read(next)
	if err != nil || n != 1 {
		t.Fatalf("read after seek: n=%d err=%v", n, err)
	}

	wantByte := byte(seekTarget % 251)

	if next[0] != wantByte {
		t.Fatalf(
			"read after seek returned %d, want %d",
			next[0], wantByte,
		)
	}
}
