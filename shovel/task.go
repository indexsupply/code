package shovel

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/indexsupply/shovel/dig"
	"github.com/indexsupply/shovel/eth"
	"github.com/indexsupply/shovel/jrpc2"
	"github.com/indexsupply/shovel/shovel/config"
	"github.com/indexsupply/shovel/shovel/glf"
	"github.com/indexsupply/shovel/wctx"
	"github.com/indexsupply/shovel/wpg"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

//go:embed schema.sql
var Schema string

type Source interface {
	Get(context.Context, string, *glf.Filter, uint64, uint64) ([]eth.Block, error)
	Latest(context.Context, string, uint64) (uint64, []byte, error)
	Hash(context.Context, string, uint64) ([]byte, error)
	NextURL() *jrpc2.URL
}

type Destination interface {
	Insert(context.Context, *sync.Mutex, wpg.Conn, []eth.Block) (int64, error)
	Delete(context.Context, wpg.Conn, uint64) error
	Filter() glf.Filter
}

type Option func(t *Task)

func WithContext(ctx context.Context) Option {
	return func(t *Task) {
		t.ctx = ctx
	}
}

func WithSrcName(name string) Option {
	return func(t *Task) {
		t.srcName = name
	}
}

func WithChainID(chainID uint64) Option {
	return func(t *Task) {
		t.srcChainID = chainID
	}
}

func WithSource(src Source) Option {
	return func(t *Task) {
		t.src = src
	}
}

func WithIntegrationFactory(f func(config.Integration) (Destination, error)) Option {
	return func(t *Task) {
		t.destFactory = f
	}
}

func WithIntegration(ig config.Integration) Option {
	return func(t *Task) {
		t.destConfig = ig
	}
}

func WithPG(pg *pgxpool.Pool) Option {
	return func(t *Task) {
		t.pgp = pg
	}
}

func WithRange(start, stop uint64) Option {
	return func(t *Task) {
		t.start, t.stop = start, stop
	}
}

func WithPollDuration(d time.Duration) Option {
	return func(t *Task) {
		t.pollDuration = d
	}
}

func WithConcurrency(concurrency, batchSize int) Option {
	return func(t *Task) {
		if concurrency > 0 {
			t.concurrency = concurrency
		}
		if batchSize > 0 {
			t.batchSize = batchSize
		}
	}
}

var compiled = map[string]Destination{}

func NewDestination(ig config.Integration) (Destination, error) {
	switch {
	case len(ig.Compiled.Name) > 0:
		dest, ok := compiled[ig.Name]
		if !ok {
			return nil, fmt.Errorf("unable to find compiled integration: %s", ig.Name)
		}
		return dest, nil
	default:
		dest, err := dig.New(ig.Name, ig.Event, ig.Block, ig.Table, ig.Notification, ig.FilterAGG)
		if err != nil {
			return nil, fmt.Errorf("building abi integration: %w", err)
		}
		return dest, nil
	}
}

func NewTask(opts ...Option) (*Task, error) {
	t := &Task{
		ctx:          context.Background(),
		pollDuration: time.Second,
		batchSize:    1,
		concurrency:  1,
		destFactory:  NewDestination,
	}
	for _, opt := range opts {
		opt(t)
	}
	t.dests = make([]Destination, t.concurrency)
	for i := 0; i < t.concurrency; i++ {
		dest, err := t.destFactory(t.destConfig)
		if err != nil {
			return nil, fmt.Errorf("initializing destination: %w", err)
		}
		t.dests[i] = dest
	}
	t.filter = t.dests[0].Filter()
	t.lockid = wpg.LockHash(fmt.Sprintf(
		"shovel-task-%s-%s",
		t.srcName,
		t.destConfig.Name,
	))
	_, err := t.pgp.Exec(t.ctx, fmt.Sprintf(
		"set application_name = 'shovel-task-%s-%s-%s'",
		t.srcName,
		t.destConfig.Name,
		wctx.Version(t.ctx),
	))
	if err != nil {
		return nil, fmt.Errorf("setting application_name: %w", err)
	}
	slog.InfoContext(t.ctx, "new-task")
	return t, nil
}

type Task struct {
	ctx context.Context
	pgp *pgxpool.Pool

	lockid       int64
	pollDuration time.Duration
	batchSize    int
	concurrency  int
	start, stop  uint64

	filter glf.Filter

	src        Source
	srcName    string
	srcChainID uint64

	dests       []Destination
	destFactory func(config.Integration) (Destination, error)
	destConfig  config.Integration
}

func (t *Task) update(
	pg wpg.Conn,
	num uint64,
	hash []byte,
	srcNum uint64,
	srcHash []byte,
	nblocks uint64,
	nrows int64,
	elapsed time.Duration,
) error {
	const uq = `
		insert into shovel.task_updates (
			chain_id,
			src_name,
			ig_name,
			num,
			hash,
			src_num,
			src_hash,
			stop,
			nblocks,
			nrows,
			latency
		)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err := pg.Exec(t.ctx, uq,
		t.srcChainID,
		t.srcName,
		t.destConfig.Name,
		num,
		hash,
		srcNum,
		srcHash,
		t.stop,
		nblocks,
		nrows,
		elapsed,
	)
	return err
}

// Delete removes the task updates at or above n and the destination's data
// for every block those updates covered.
//
// [Task.update] writes one shovel.task_updates row per converge batch and sets
// num to the batch's LAST block, so a single update covers (previous update,
// num]. Deleting data at >= n only would therefore keep the deleted batch's
// canonical prefix -- blocks below n that no remaining update accounts for.
// Converge resumes from the highest remaining update and requests those blocks
// again, and since [Destination.Insert] uses COPY (no on conflict), the retry
// fails against the destination's unique index forever: the task stops making
// progress until the leftover rows are deleted by hand.
func (t *Task) Delete(pg wpg.Conn, n uint64) error {
	const q = `
		delete from shovel.task_updates
		where src_name = $1
		and ig_name = $2
		and num >= $3
	`
	cmd, err := pg.Exec(t.ctx, q, t.srcName, t.destConfig.Name, n)
	if err != nil {
		return fmt.Errorf("deleting block from task table: %w", err)
	}
	dn, err := t.resume(pg, n)
	if err != nil {
		return err
	}
	err = t.dests[0].Delete(t.ctx, pg, dn)
	if err != nil {
		return fmt.Errorf("deleting block: %w", err)
	}
	slog.InfoContext(t.ctx, "task-delete",
		"n", n,
		"data_n", dn,
		"task_updates", cmd.RowsAffected(),
	)
	return nil
}

// resume reports the first block Converge will request once the task updates
// at or above n are gone: one past the highest remaining update. It is never
// greater than n, since no update at or above n survives the delete.
//
// With no updates left the task restarts at its configured start, or -- absent
// one -- at the chain head, which is above anything already stored. Both cases
// fall back to n rather than deleting the destination's whole history.
func (t *Task) resume(pg wpg.Conn, n uint64) (uint64, error) {
	const q = `
		select coalesce(max(num) + 1, 0)
		from shovel.task_updates
		where src_name = $1
		and ig_name = $2
	`
	var num uint64
	err := pg.QueryRow(t.ctx, q, t.srcName, t.destConfig.Name).Scan(&num)
	if err != nil {
		return 0, fmt.Errorf("querying remaining task updates: %w", err)
	}
	switch {
	case num > 0:
		return num, nil
	case t.start > 0:
		return t.start, nil
	default:
		return n, nil
	}
}

func (t *Task) latestDependency(pg wpg.Conn) (uint64, []byte, error) {
	const q = `
		with latest as (
			select distinct on (ig_name)
			ig_name, num, hash
			from shovel.task_updates
			where src_name = $1
			and ig_name = ANY($2)
			order by ig_name, num desc
		)
		select num, hash
		from latest
		order by num asc
		limit 1;
	`
	num, hash := uint64(0), []byte{}
	err := pg.QueryRow(
		t.ctx,
		q,
		t.srcName,
		t.destConfig.Dependencies,
	).Scan(&num, &hash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nil, nil
	case err != nil:
		return 0, nil, err
	default:
		return num, hash, nil
	}
}

func (t *Task) latest(ctx context.Context, pg wpg.Conn) (uint64, []byte, error) {
	const q = `
		select num, hash
		from shovel.task_updates
		where src_name = $1
		and ig_name = $2
		order by num desc
		limit 1
	`
	localNum, localHash := uint64(0), []byte{}
	err := pg.QueryRow(
		t.ctx,
		q,
		t.srcName,
		t.destConfig.Name,
	).Scan(&localNum, &localHash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		switch {
		case t.start > 0:
			n := t.start - 1
			h, err := t.src.Hash(ctx, t.src.NextURL().String(), n)
			if err != nil {
				return 0, nil, fmt.Errorf("getting hash for %d: %w", n, err)
			}
			slog.InfoContext(t.ctx, "start at config", "num", t.start)
			return n, h, nil
		default:
			n, _, err := t.src.Latest(ctx, t.src.NextURL().String(), 0)
			if err != nil {
				return 0, nil, err
			}
			h, err := t.src.Hash(ctx, t.src.NextURL().String(), n-1)
			if err != nil {
				return 0, nil, fmt.Errorf("getting hash for %d: %w", n-1, err)
			}
			slog.InfoContext(t.ctx, "start at latest", "num", n)
			return n - 1, h, nil
		}
	case err != nil:
		return 0, nil, fmt.Errorf("querying for latest: %w", err)
	default:
		return localNum, localHash, nil
	}
}

var (
	ErrNothingNew = errors.New("no new blocks")
	ErrReorg      = errors.New("reorg")
	ErrDone       = errors.New("this is the end")
	ErrAhead      = errors.New("ahead")
)

// Indexes at most batchSize of the delta between min(g, limit) and pg.
// If pg contains an invalid latest block (ie reorg) then [ErrReorg]
// is returned and the caller may rollback the transaction resulting
// in no side-effects.
func (task *Task) Converge() error {
	var (
		ctx     = task.ctx
		t0      = time.Now()
		nextURL = task.src.NextURL()
		url     = nextURL.String()
		nrpc    = uint64(0)
	)
	ctx = wctx.WithSrcHost(ctx, nextURL.Hostname())
	ctx = wctx.WithCounter(ctx, &nrpc)

	pgtx, err := task.pgp.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to start tx: %w", err)
	}
	defer pgtx.Rollback(ctx)

	for reorgs := 0; reorgs <= 1000; reorgs++ {
		localNum, localHash, err := task.latest(ctx, pgtx)
		if err != nil {
			return fmt.Errorf("getting latest from task: %w", err)
		}
		if task.stop > 0 && localNum >= task.stop {
			return ErrDone
		}
		gethNum, gethHash, err := task.src.Latest(ctx, url, localNum)
		if err != nil {
			return fmt.Errorf("getting latest from eth: %w", err)
		}
		var (
			targetNum  uint64
			targetHash []byte
		)
		switch {
		case len(task.destConfig.Dependencies) > 0:
			depNum, depHash, err := task.latestDependency(pgtx)
			if err != nil {
				return fmt.Errorf("getting latest from dependencies: %w", err)
			}
			switch {
			case depNum == 0:
				return ErrNothingNew
			case depNum < gethNum:
				targetNum = depNum
				targetHash = depHash
			default:
				targetNum = gethNum
				targetHash = gethHash
			}
		default:
			targetNum = gethNum
			targetHash = gethHash
		}
		if task.stop > 0 && targetNum > task.stop {
			targetNum = task.stop
		}
		if localNum > targetNum {
			slog.ErrorContext(ctx, "ahead",
				"local", localNum,
				"remote", targetNum,
			)
			return ErrAhead
		}
		if localNum == targetNum {
			return ErrNothingNew
		}
		delta := min(targetNum-localNum, uint64(task.batchSize))
		if delta == 0 {
			return ErrNothingNew
		}
		ctx = wctx.WithNumLimit(ctx, localNum+1, delta)
		blocks, err := task.load(ctx, url, localHash, localNum+1, delta)
		if errors.Is(err, ErrReorg) {
			slog.ErrorContext(ctx, "reorg",
				"n", localNum,
				"h", fmt.Sprintf("%.4x", localHash),
			)
			if err := task.Delete(pgtx, localNum); err != nil {
				return fmt.Errorf("deleting during reorg: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("loading data: %w", err)
		}
		if err := pgtx.Commit(ctx); err != nil {
			return fmt.Errorf("comitting task_updates tx: %w", err)
		}

		pgtx, err = task.pgp.Begin(ctx)
		if err != nil {
			return fmt.Errorf("starting insert pg tx: %w", err)
		}
		nrows, err := task.insert(ctx, pgtx, blocks)
		if err != nil {
			pgtx.Rollback(ctx)
			return fmt.Errorf("inserting data: %w", err)
		}
		last := blocks[len(blocks)-1]
		err = task.update(pgtx, last.Num(), last.Hash(), targetNum, targetHash, delta, nrows, time.Since(t0))
		if err != nil {
			pgtx.Rollback(ctx)
			return fmt.Errorf("updating task: %w", err)
		}
		if err := pgtx.Commit(ctx); err != nil {
			return fmt.Errorf("committing task tx: %w", err)
		}
		slog.InfoContext(ctx, "converge",
			"n", last.Num(),
			"h", fmt.Sprintf("%.4x", last.Hash()),
			"nrows", nrows,
			"nrpc", wctx.Counter(ctx),
			"nblocks", delta,
			"elapsed", time.Since(t0),
		)
		return nil
	}
	return ErrReorg
}

func (t *Task) load(
	ctx context.Context,
	url string,
	localHash []byte,
	start, limit uint64,
) ([]eth.Block, error) {
	var (
		t0   = time.Now()
		eg   errgroup.Group
		part = t.batchSize / t.concurrency

		blocksMut sync.Mutex
		blocks    []eth.Block
	)
	for i := 0; i < t.concurrency; i++ {
		i := i
		m := start + uint64(i*part)
		n := min(uint64(part), limit-uint64(i*part))
		if m > start+limit || n == 0 {
			continue
		}
		eg.Go(func() error {
			ctx = wctx.WithNumLimit(ctx, m, n)
			b, err := t.src.Get(ctx, url, &t.filter, m, n)
			if err != nil {
				slog.ErrorContext(ctx, "loading blocks", "error", err)
				return fmt.Errorf("loading blocks: %w", err)
			}
			blocksMut.Lock()
			blocks = append(blocks, b...)
			blocksMut.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	slices.SortFunc(blocks, func(a, b eth.Block) int {
		return cmp.Compare(a.Num(), b.Num())
	})
	first, last := blocks[0], blocks[len(blocks)-1]
	if len(first.Header.Parent) == 32 && !bytes.Equal(localHash, first.Header.Parent) {
		return nil, ErrReorg
	}
	slog.DebugContext(ctx, "load",
		"n", last.Num(),
		"h", fmt.Sprintf("%.4x", last.Hash()),
		"nrpc", wctx.Counter(ctx),
		"elapsed", time.Since(t0),
	)
	return blocks, nil
}

func (t *Task) insert(
	ctx context.Context,
	pg wpg.Conn,
	blocks []eth.Block,
) (int64, error) {
	var (
		t0    = time.Now()
		nrows int64
		eg    errgroup.Group
		pgmut sync.Mutex
	)
	for i := 0; i < len(blocks); i += t.batchSize {
		i := i
		n := i + t.batchSize
		if n > len(blocks) {
			n = len(blocks)
		}
		eg.Go(func() error {
			ctx = wctx.WithNumLimit(ctx, uint64(i), uint64(n))
			nr, err := t.dests[i].Insert(ctx, &pgmut, pg, blocks[i:n])
			if err != nil {
				return fmt.Errorf("inserting blocks: %w", err)
			}
			atomic.AddInt64(&nrows, nr)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return 0, err
	}
	last := blocks[len(blocks)-1]
	slog.DebugContext(ctx, "insert",
		"n", last.Num(),
		"h", fmt.Sprintf("%.4x", last.Hash()),
		"nrows", nrows,
		"elapsed", time.Since(t0),
	)
	return nrows, nil
}

func PruneTask(ctx context.Context, pg wpg.Conn, n int) error {
	const q = `
		delete from shovel.task_updates
		where (src_name, ig_name, num) not in (
			select src_name, ig_name, num
			from (
				select
					src_name,
					ig_name,
					num,
					row_number() over(partition by src_name, ig_name order by num desc) as rn
				from shovel.task_updates
			) as s
			where rn <= $1
		)
	`
	cmd, err := pg.Exec(ctx, q, n)
	if err != nil {
		return fmt.Errorf("deleting shovel.task_updates: %w", err)
	}
	slog.InfoContext(ctx, "prune-task", "n", cmd.RowsAffected())
	return nil
}

type jsonDuration time.Duration

func (d *jsonDuration) ScanInterval(i pgtype.Interval) error {
	*d = jsonDuration(i.Microseconds * 1000)
	return nil
}

func (d *jsonDuration) UnmarshalJSON(data []byte) error {
	if len(data) < 2 {
		return fmt.Errorf("jsonDuration must be at leaset 2 bytes")
	}
	data = data[1 : len(data)-1] // remove quotes
	dur, err := time.ParseDuration(string(data))
	*d = jsonDuration(dur)
	return err
}

func (d jsonDuration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d.String())), nil
}

func (d jsonDuration) String() string {
	switch td := time.Duration(d); {
	case td < 100*time.Millisecond:
		return td.Truncate(time.Millisecond).String()
	case td < time.Second:
		return td.Truncate(100 * time.Millisecond).String()
	default:
		return td.Truncate(time.Second).String()
	}
}

type SrcUpdate struct {
	DOMID   string        `db:"-"`
	Name    string        `db:"src_name"`
	Num     sql.NullInt64 `db:"num"`
	Hash    eth.Bytes     `db:"hash"`
	SrcNum  sql.NullInt64 `db:"src_num"`
	SrcHash eth.Bytes     `db:"src_hash"`
	NBlocks sql.NullInt64 `db:"nblocks"`
	NRows   sql.NullInt64 `db:"nrows"`
	Latency jsonDuration  `db:"latency"`
}

func SourceUpdates(ctx context.Context, pg wpg.Conn) ([]SrcUpdate, error) {
	rows, _ := pg.Query(ctx, `select * from shovel.source_updates`)
	updates, err := pgx.CollectRows(rows, pgx.RowToStructByName[SrcUpdate])
	if err != nil {
		return nil, fmt.Errorf("querying for source updates: %w", err)
	}
	return updates, nil
}

type TaskUpdate struct {
	DOMID    string       `db:"-"`
	SrcName  string       `db:"src_name"`
	DestName string       `db:"ig_name"`
	Num      uint64       `db:"num"`
	Stop     uint64       `db:"stop"`
	Hash     eth.Bytes    `db:"hash"`
	SrcNum   uint64       `db:"src_num"`
	SrcHash  eth.Bytes    `db:"src_hash"`
	NBlocks  uint64       `db:"nblocks"`
	NRows    uint64       `db:"nrows"`
	Latency  jsonDuration `db:"latency"`
}

func TaskUpdates(ctx context.Context, pg wpg.Conn) ([]TaskUpdate, error) {
	rows, _ := pg.Query(ctx, `
        with f as (
            select src_name, ig_name, max(num) num
            from shovel.task_updates group by 1, 2
        )
        select
			f.src_name,
			f.ig_name,
			f.num,
			coalesce(stop, 0) stop,
			hash,
			coalesce(src_num, 0) src_num,
			coalesce(src_hash, '\x00') src_hash,
			coalesce(nblocks, 0) nblocks,
			coalesce(nrows, 0) nrows,
			coalesce(latency, '0')::interval latency
        from f
        left join shovel.task_updates
		on shovel.task_updates.src_name = f.src_name
		and shovel.task_updates.ig_name= f.ig_name
		and shovel.task_updates.num = f.num;
    `)
	tus, err := pgx.CollectRows(rows, pgx.RowToStructByName[TaskUpdate])
	if err != nil {
		return nil, fmt.Errorf("querying for task updates: %w", err)
	}
	for i := range tus {
		tus[i].DOMID = fmt.Sprintf("%s-%s", tus[i].SrcName, tus[i].DestName)
	}
	return tus, nil
}

// Loads, Starts, and provides method for Restarting tasks
// based on config stored in the DB and in the config file.
type Manager struct {
	ctx     context.Context
	running sync.Mutex
	restart chan struct{}
	tasks   []*Task
	updates chan uint64
	pgp     *pgxpool.Pool
	conf    config.Root
}

func NewManager(ctx context.Context, pgp *pgxpool.Pool, conf config.Root) *Manager {
	return &Manager{
		ctx:     ctx,
		restart: make(chan struct{}),
		updates: make(chan uint64),
		pgp:     pgp,
		conf:    conf,
	}
}

func (tm *Manager) Updates() uint64 {
	return <-tm.updates
}

func (tm *Manager) runTask(t *Task) {
	for {
		select {
		case <-tm.restart:
			slog.InfoContext(t.ctx, "restart-task")
			return
		default:
			switch err := t.Converge(); {
			case errors.Is(err, ErrDone):
				slog.InfoContext(t.ctx, "done")
				return
			case errors.Is(err, ErrNothingNew):
				time.Sleep(t.pollDuration)
			case err != nil:
				time.Sleep(time.Second)
				slog.ErrorContext(t.ctx, "converge-retry", "msg", err)
			default:
				go func() {
					// try out best to deliver update
					// but don't stack up work
					select {
					case tm.updates <- t.srcChainID:
					default:
					}
				}()
			}
		}
	}
}

// Ensures all running tasks stop
// and calls [Manager.Run] in a new go routine.
func (tm *Manager) Restart() error {
	close(tm.restart)
	ec := make(chan error)
	go tm.Run(ec)
	return <-ec
}

// Loads ethereum sources and integrations from both the config file
// and the database and assembles the nessecary tasks and runs all
// tasks in a loop.
//
// Acquires a lock to ensure only on routine is running.
// Releases lock on return
func (tm *Manager) Run(ec chan error) {
	tm.running.Lock()
	defer tm.running.Unlock()

	var err error
	tm.tasks, err = loadTasks(tm.ctx, tm.pgp, tm.conf)
	if err != nil {
		ec <- fmt.Errorf("loading tasks: %w", err)
		return
	}
	close(ec)

	tm.restart = make(chan struct{})
	var wg sync.WaitGroup
	for i := range tm.tasks {
		i := i
		wg.Add(1)
		go func() {
			tm.runTask(tm.tasks[i])
			wg.Done()
		}()
	}
	wg.Wait()
}

func loadTasks(ctx context.Context, pgp *pgxpool.Pool, c config.Root) ([]*Task, error) {
	allIntegrations, err := c.AllIntegrations(ctx, pgp)
	if err != nil {
		return nil, fmt.Errorf("loading integrations: %w", err)
	}
	scByName, err := c.AllSourcesByName(ctx, pgp)
	if err != nil {
		return nil, fmt.Errorf("loading source configs: %w", err)
	}
	var sources = map[string]Source{}
	for _, sc := range scByName {
		sources[sc.Name] = jrpc2.New(sc.URLs...).
			WithWSURL(sc.WSURL).
			WithPollDuration(sc.PollDuration).
			WithMaxReads(len(allIntegrations))
	}
	var tasks []*Task
	for _, ig := range allIntegrations {
		if !ig.Enabled {
			continue
		}
		for _, scRef := range ig.Sources {
			sc, ok := scByName[scRef.Name]
			if !ok {
				return nil, fmt.Errorf("finding source config for %s", scRef.Name)
			}
			ctx = wctx.WithChainID(ctx, sc.ChainID)
			ctx = wctx.WithSrcName(ctx, sc.Name)
			ctx = wctx.WithIGName(ctx, ig.Name)
			src, ok := sources[scRef.Name]
			if !ok {
				return nil, fmt.Errorf("finding source for %s", scRef.Name)
			}
			task, err := NewTask(
				WithContext(ctx),
				WithPG(pgp),
				WithRange(scRef.Start, scRef.Stop),
				WithPollDuration(sc.PollDuration),
				WithConcurrency(sc.Concurrency, sc.BatchSize),
				WithSrcName(sc.Name),
				WithChainID(sc.ChainID),
				WithSource(src),
				WithIntegration(ig),
			)
			if err != nil {
				return nil, fmt.Errorf("setting up main task: %w", err)
			}
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}
