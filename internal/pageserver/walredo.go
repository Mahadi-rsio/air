package pageserver

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	defaultRedoWorkers        = 4
	maxRedoWorkers            = 16
	defaultRedoQueueSize      = 64
	defaultRedoRequestTimeout = 10 * time.Second
)

var (
	ErrWALRedoClosed = errors.New("pageserver: walredo worker pool closed")
)

// WALRedoer reconstructs an 8192-byte page from an optional base page and ordered WAL deltas.
type WALRedoer interface {
	Redo(ctx context.Context, key Key, basePage []byte, deltas []WALDelta) ([]byte, error)
}

// WALRedoRequest describes a single page reconstruction request for batch WAL redo.
type WALRedoRequest struct {
	Key      Key
	BasePage []byte
	Deltas   []WALDelta
}

// BatchWALRedoer is an optional extension of WALRedoer that executes multiple page redos concurrently.
type BatchWALRedoer interface {
	WALRedoer
	RedoBatch(ctx context.Context, reqs []WALRedoRequest) ([][]byte, []error)
}

// WALRedoPoolConfig configures the bounded persistent WAL-redo worker pool.
type WALRedoPoolConfig struct {
	WorkerBin      string
	Workers        int
	QueueSize      int
	RequestTimeout time.Duration
	Env            []string
}

type redoJob struct {
	ctx      context.Context
	key      Key
	basePage []byte
	records  [][]byte
	resCh    chan redoResult
}

type redoResult struct {
	page []byte
	err  error
}

// DefaultWALRedoer applies FPIs directly in Go and delegates non-FPI delta chains
// to a bounded pool of persistent external PG17 walredo worker processes.
type DefaultWALRedoer struct {
	cfg      WALRedoPoolConfig
	initOnce sync.Once
	reqCh    chan *redoJob
	closedCh chan struct{}
	closeMu  sync.Mutex
	closed   bool
	wg       sync.WaitGroup
}

// NewDefaultWALRedoer creates a WALRedoer with default bounded worker pool settings.
// If workerBin is empty, only FPI/will-init records are applied.
func NewDefaultWALRedoer(workerBin string) *DefaultWALRedoer {
	return NewWALRedoPool(WALRedoPoolConfig{
		WorkerBin: workerBin,
	})
}

// NewWALRedoPool creates a WALRedoer backed by a bounded persistent worker pool.
func NewWALRedoPool(cfg WALRedoPoolConfig) *DefaultWALRedoer {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
		if cfg.Workers > defaultRedoWorkers {
			cfg.Workers = defaultRedoWorkers
		}
		if cfg.Workers < 1 {
			cfg.Workers = 1
		}
	}
	if cfg.Workers > maxRedoWorkers {
		cfg.Workers = maxRedoWorkers
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultRedoQueueSize
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultRedoRequestTimeout
	}
	return &DefaultWALRedoer{
		cfg:      cfg,
		closedCh: make(chan struct{}),
	}
}

func (r *DefaultWALRedoer) ensureStarted() {
	r.initOnce.Do(func() {
		r.reqCh = make(chan *redoJob, r.cfg.QueueSize)
		for i := 0; i < r.cfg.Workers; i++ {
			r.wg.Add(1)
			go r.workerLoop()
		}
	})
}

// Close shuts down the persistent worker pool and terminates all child processes.
func (r *DefaultWALRedoer) Close() error {
	r.closeMu.Lock()
	if r.closed {
		r.closeMu.Unlock()
		return nil
	}
	r.closed = true
	close(r.closedCh)
	r.closeMu.Unlock()
	r.wg.Wait()
	return nil
}

// prepareRedo resolves FPI/will-init records in Go and returns either a completed page
// or the pending non-FPI WAL records that require the external worker.
func prepareRedo(key Key, basePage []byte, deltas []WALDelta) (cur []byte, pendingWAL [][]byte, done bool, err error) {
	if len(basePage) == PageSize {
		cur = append([]byte(nil), basePage...)
	}
	for _, d := range deltas {
		if (d.Flags&EntryFlagPageImage) != 0 && len(d.PageImage) == PageSize {
			cur = append([]byte(nil), d.PageImage...)
			pendingWAL = nil
			continue
		}
		if (d.Flags & EntryFlagWillInit) != 0 {
			cur = make([]byte, PageSize)
		}
		if (d.Flags&EntryFlagWALRecord) != 0 && len(d.WALRecord) == 0 {
			return nil, nil, true, ErrTruncatedWAL
		}
		if len(d.WALRecord) > 0 {
			pendingWAL = append(pendingWAL, d.WALRecord)
		}
	}
	if len(pendingWAL) == 0 {
		if len(cur) == PageSize {
			return cur, nil, true, nil
		}
		return nil, nil, true, ErrNotFound
	}
	return cur, pendingWAL, false, nil
}

// Redo applies deltas onto basePage in LSN order and returns the materialized 8192-byte page.
func (r *DefaultWALRedoer) Redo(ctx context.Context, key Key, basePage []byte, deltas []WALDelta) ([]byte, error) {
	start := time.Now()
	cur, pendingWAL, done, err := prepareRedo(key, basePage, deltas)
	if done {
		debugf("walredo", "Redo fast_path key=%s deltas=%d has_base=%v err=%v dur=%s", key, len(deltas), len(basePage) == PageSize, err, time.Since(start))
		return cur, err
	}
	if r.cfg.WorkerBin == "" {
		if applied, ok := ApplyWALRecordsInGo(key, cur, pendingWAL); ok && len(applied) == PageSize {
			debugf("walredo", "Redo in_go key=%s pending_wal=%d dur=%s", key, len(pendingWAL), time.Since(start))
			return applied, nil
		}
		var rmid uint8
		var rinfo uint8
		if len(pendingWAL) > 0 {
			if rec0, rerr := DecodeWALRecord(0, pendingWAL[0]); rerr == nil {
				rmid = rec0.Rmid
				rinfo = rec0.Info
			}
		}
		return nil, fmt.Errorf("pageserver: %d non-FPI WAL records (rmid=%d info=0x%02x) for %s require walredo worker", len(pendingWAL), rmid, rinfo, key)
	}
	page, err := r.submitJob(ctx, key, cur, pendingWAL)
	debugf("walredo", "Redo external_worker key=%s pending_wal=%d err=%v dur=%s", key, len(pendingWAL), err, time.Since(start))
	return page, err
}

// RedoBatch executes multiple WAL redo requests concurrently across the bounded worker pool.
func (r *DefaultWALRedoer) RedoBatch(ctx context.Context, reqs []WALRedoRequest) ([][]byte, []error) {
	pages := make([][]byte, len(reqs))
	errs := make([]error, len(reqs))
	if len(reqs) == 0 {
		return pages, errs
	}

	type inflight struct {
		idx   int
		resCh chan redoResult
	}
	var pending []inflight

	for i, req := range reqs {
		cur, pendingWAL, done, err := prepareRedo(req.Key, req.BasePage, req.Deltas)
		if done {
			pages[i] = cur
			errs[i] = err
			continue
		}
		if r.cfg.WorkerBin == "" {
			if applied, ok := ApplyWALRecordsInGo(req.Key, cur, pendingWAL); ok && len(applied) == PageSize {
				pages[i] = applied
			} else {
				var rmid uint8
				var rinfo uint8
				if len(pendingWAL) > 0 {
					if rec0, rerr := DecodeWALRecord(0, pendingWAL[0]); rerr == nil {
						rmid = rec0.Rmid
						rinfo = rec0.Info
					}
				}
				errs[i] = fmt.Errorf("pageserver: %d non-FPI WAL records (rmid=%d info=0x%02x) for %s require walredo worker", len(pendingWAL), rmid, rinfo, req.Key)
			}
			continue
		}

		resCh, subErr := r.enqueueJob(ctx, req.Key, cur, pendingWAL)
		if subErr != nil {
			errs[i] = subErr
			continue
		}
		pending = append(pending, inflight{idx: i, resCh: resCh})
	}

	for _, p := range pending {
		select {
		case res := <-p.resCh:
			pages[p.idx] = res.page
			errs[p.idx] = res.err
		case <-ctx.Done():
			errs[p.idx] = ctx.Err()
		case <-r.closedCh:
			errs[p.idx] = ErrWALRedoClosed
		}
	}
	return pages, errs
}

func (r *DefaultWALRedoer) submitJob(ctx context.Context, key Key, basePage []byte, records [][]byte) ([]byte, error) {
	resCh, err := r.enqueueJob(ctx, key, basePage, records)
	if err != nil {
		return nil, err
	}
	select {
	case res := <-resCh:
		return res.page, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.closedCh:
		return nil, ErrWALRedoClosed
	}
}

func (r *DefaultWALRedoer) enqueueJob(ctx context.Context, key Key, basePage []byte, records [][]byte) (chan redoResult, error) {
	r.closeMu.Lock()
	if r.closed {
		r.closeMu.Unlock()
		return nil, ErrWALRedoClosed
	}
	r.ensureStarted()
	r.closeMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	job := &redoJob{
		ctx:      ctx,
		key:      key,
		basePage: basePage,
		records:  records,
		resCh:    make(chan redoResult, 1),
	}
	select {
	case r.reqCh <- job:
		return job.resCh, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.closedCh:
		return nil, ErrWALRedoClosed
	}
}

type cappedStderr struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (c *cappedStderr) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.max <= 0 {
		c.max = 2048
	}
	if len(c.buf) < c.max {
		rem := c.max - len(c.buf)
		if len(p) > rem {
			c.buf = append(c.buf, p[:rem]...)
		} else {
			c.buf = append(c.buf, p...)
		}
	}
	return len(p), nil
}

func (c *cappedStderr) ResetAndString() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := strings.TrimSpace(string(c.buf))
	c.buf = c.buf[:0]
	return s
}

func (c *cappedStderr) Reset() {
	c.mu.Lock()
	c.buf = c.buf[:0]
	c.mu.Unlock()
}

type persistentWorker struct {
	bin    string
	env    []string
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	bw     *bufio.Writer
	stdout io.ReadCloser
	stderr cappedStderr
}

func (w *persistentWorker) start() error {
	cmd := exec.Command(w.bin)
	if len(w.env) > 0 {
		cmd.Env = append(cmd.Environ(), w.env...)
	}
	w.stderr.Reset()
	cmd.Stderr = &w.stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return err
	}
	w.cmd = cmd
	w.stdin = stdin
	w.bw = bufio.NewWriterSize(stdin, 16*1024)
	w.stdout = stdout
	return nil
}

func (w *persistentWorker) kill() {
	if w.stdin != nil {
		_ = w.stdin.Close()
		w.stdin = nil
	}
	if w.stdout != nil {
		_ = w.stdout.Close()
		w.stdout = nil
	}
	if w.cmd != nil {
		if w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
		_ = w.cmd.Wait()
		w.cmd = nil
	}
	w.bw = nil
}

func (w *persistentWorker) execJob(job *redoJob, defaultTimeout time.Duration) ([]byte, error) {
	if err := job.ctx.Err(); err != nil {
		return nil, err
	}
	if w.cmd == nil {
		if err := w.start(); err != nil {
			return nil, err
		}
	}

	// Enforce cancellation / timeout by killing the worker if the request aborts mid-flight.
	var procMu sync.Mutex
	var waited bool
	proc := w.cmd.Process
	killProc := func() {
		procMu.Lock()
		if !waited && proc != nil {
			_ = proc.Kill()
		}
		procMu.Unlock()
	}

	doneCh := make(chan struct{})
	var stopTimer *time.Timer
	if _, hasDeadline := job.ctx.Deadline(); !hasDeadline && defaultTimeout > 0 {
		stopTimer = time.AfterFunc(defaultTimeout, killProc)
	}
	var cancelWg sync.WaitGroup
	cancelWg.Add(1)
	go func() {
		defer cancelWg.Done()
		select {
		case <-job.ctx.Done():
			killProc()
		case <-doneCh:
		}
	}()

	w.stderr.Reset()
	err := writeRedoFrame(w.bw, job.key, job.basePage, job.records)
	if err == nil {
		err = w.bw.Flush()
	}
	var out []byte
	if err == nil {
		out = make([]byte, PageSize)
		_, err = io.ReadFull(w.stdout, out)
	}

	close(doneCh)
	cancelWg.Wait()
	if stopTimer != nil {
		stopTimer.Stop()
	}

	if err != nil {
		procMu.Lock()
		waited = true
		procMu.Unlock()
		w.kill()
		stderrMsg := w.stderr.ResetAndString()
		if ctxErr := job.ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if stderrMsg != "" {
			return nil, fmt.Errorf("pageserver: walredo worker failed: %w (stderr: %s)", err, stderrMsg)
		}
		return nil, errors.Join(errors.New("pageserver: walredo worker failed"), err)
	}
	return out, nil
}

func writeRedoFrame(w io.Writer, key Key, basePage []byte, records [][]byte) error {
	// Frame: [24B key][1B hasBase][8192B basePage if hasBase][4B count]([4B len][record])...
	var keyBuf [24]byte
	key.Encode(keyBuf[:])
	if _, err := w.Write(keyBuf[:]); err != nil {
		return err
	}
	if len(basePage) == PageSize {
		if _, err := w.Write([]byte{1}); err != nil {
			return err
		}
		if _, err := w.Write(basePage); err != nil {
			return err
		}
	} else {
		if _, err := w.Write([]byte{0}); err != nil {
			return err
		}
	}
	var cnt [4]byte
	binary.LittleEndian.PutUint32(cnt[:], uint32(len(records)))
	if _, err := w.Write(cnt[:]); err != nil {
		return err
	}
	for _, rec := range records {
		binary.LittleEndian.PutUint32(cnt[:], uint32(len(rec)))
		if _, err := w.Write(cnt[:]); err != nil {
			return err
		}
		if _, err := w.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

func (r *DefaultWALRedoer) workerLoop() {
	defer r.wg.Done()
	w := &persistentWorker{
		bin: r.cfg.WorkerBin,
		env: r.cfg.Env,
	}
	defer w.kill()

	for {
		select {
		case <-r.closedCh:
			return
		case job := <-r.reqCh:
			page, err := w.execJob(job, r.cfg.RequestTimeout)
			job.resCh <- redoResult{page: page, err: err}
		}
	}
}
