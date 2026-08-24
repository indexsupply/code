package shovel

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/indexsupply/shovel/dig"
	"github.com/indexsupply/shovel/eth"
	"github.com/indexsupply/shovel/jrpc2"
	"github.com/indexsupply/shovel/shovel/config"
	"github.com/indexsupply/shovel/shovel/glf"
	"github.com/indexsupply/shovel/tc"
	"github.com/indexsupply/shovel/wpg"

	"blake.io/pqx/pqxtest"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"kr.dev/diff"
)

func TestMain(m *testing.M) {
	sql.Register("postgres", stdlib.GetDefaultDriver())
	pqxtest.TestMain(m)
}

type testDestination struct {
	sync.Mutex
	name  string
	chain map[uint64]eth.Block
}

func (dest *testDestination) factory(ig config.Integration) (Destination, error) {
	return dest, nil
}

func (dest *testDestination) ig() config.Integration {
	return config.Integration{Name: dest.name, Enabled: true}
}

func newTestDestination(name string) *testDestination {
	return &testDestination{
		name:  name,
		chain: make(map[uint64]eth.Block),
	}
}

func (dest *testDestination) blocks() []eth.Block {
	var blks []eth.Block
	for _, b := range dest.chain {
		blks = append(blks, b)
	}
	sort.Slice(blks, func(i, j int) bool {
		return blks[i].Num() < blks[j].Num()
	})
	return blks
}

func (dest *testDestination) Insert(_ context.Context, _ *sync.Mutex, _ wpg.Conn, blocks []eth.Block) (int64, error) {
	dest.Lock()
	defer dest.Unlock()
	for _, b := range blocks {
		// [dig.Integration.Insert] copies into a table with a unique index
		// and has no on conflict clause, so re-inserting a block that was
		// never deleted is an error rather than an overwrite.
		if _, ok := dest.chain[b.Num()]; ok {
			return 0, fmt.Errorf("duplicate key: %d", b.Num())
		}
		dest.chain[uint64(b.Header.Number)] = b
	}
	return int64(len(blocks)), nil
}

func (dest *testDestination) add(n uint64, hash, parent []byte) {
	dest.Insert(context.Background(), nil, nil, []eth.Block{
		eth.Block{
			Header: eth.Header{
				Number: eth.Uint64(n),
				Hash:   hash,
				Parent: parent,
			},
		},
	})
}

func (dest *testDestination) Delete(_ context.Context, pg wpg.Conn, n uint64) error {
	dest.Lock()
	defer dest.Unlock()
	// [dig.Integration.Delete] deletes at block_num >= n, not just n.
	for num := range dest.chain {
		if num >= n {
			delete(dest.chain, num)
		}
	}
	return nil
}

func (dest *testDestination) Filter() glf.Filter {
	return glf.Filter{UseBlocks: true, UseLogs: true}
}

type testGeth struct {
	blocks []eth.Block
}

func (tg *testGeth) NextURL() *jrpc2.URL {
	return jrpc2.MustURL("")
}

func (tg *testGeth) factory(config.Source, glf.Filter) Source {
	return tg
}

func (tg *testGeth) Hash(_ context.Context, _ string, n uint64) ([]byte, error) {
	for j := range tg.blocks {
		if uint64(tg.blocks[j].Header.Number) == n {
			return tg.blocks[j].Header.Hash, nil
		}
	}
	return nil, fmt.Errorf("not found: %d", n)
}

func (tg *testGeth) Latest(_ context.Context, _ string, _ uint64) (uint64, []byte, error) {
	if len(tg.blocks) == 0 {
		return 0, nil, nil
	}
	b := tg.blocks[len(tg.blocks)-1]
	return b.Num(), b.Hash(), nil
}

func (tg *testGeth) Get(_ context.Context, _ string, filter *glf.Filter, start, limit uint64) ([]eth.Block, error) {
	if start+limit-1 > tg.blocks[len(tg.blocks)-1].Num() {
		const tag = "no blocks. start=%d limit=%d latest=%d"
		return nil, fmt.Errorf(tag, start, limit, tg.blocks[len(tg.blocks)-1].Num())
	}
	var res []eth.Block
	for i := uint64(0); i < limit; i++ {
		for j := range tg.blocks {
			if start+i == uint64(tg.blocks[j].Num()) {
				res = append(res, tg.blocks[j])
			}
		}
	}
	return res, nil
}

func (tg *testGeth) add(n uint64, h, p []byte) {
	tg.blocks = append(tg.blocks, eth.Block{
		Header: eth.Header{
			Number: eth.Uint64(n),
			Hash:   h,
			Parent: p,
		},
	})
}

func hash(b byte) []byte {
	res := make([]byte, 32)
	res[0] = b
	return res
}

func testpg(t *testing.T) *pgxpool.Pool {
	pqxtest.CreateDB(t, Schema)
	pg, err := pgxpool.New(context.Background(), pqxtest.DSNForTest(t))
	tc.NoErr(t, err)
	return pg
}

func checkQuery(tb testing.TB, pg wpg.Conn, query string, args ...any) {
	var found bool
	err := pg.QueryRow(context.Background(), query, args...).Scan(&found)
	diff.Test(tb, tb.Fatalf, err, nil)
	if !found {
		tb.Errorf("query\n%s\nreturned false", query)
	}
}

func TestConverge_EmptyDestination(t *testing.T) {
	var (
		pg        = testpg(t)
		tg        = &testGeth{}
		dest      = newTestDestination("foo")
		task, err = NewTask(
			WithPG(pg),
			WithSource(tg),
			WithIntegration(dest.ig()),
			WithIntegrationFactory(dest.factory),
		)
	)
	diff.Test(t, t.Fatalf, err, nil)

	tg.add(0, hash(0), hash(0))
	tg.add(1, hash(1), hash(0))
	dest.add(0, hash(0), hash(0))

	diff.Test(t, t.Fatalf, task.Converge(), nil)
	diff.Test(t, t.Errorf, dest.blocks(), tg.blocks)
}

func TestConverge_Reorg(t *testing.T) {
	var (
		pg        = testpg(t)
		tg        = &testGeth{}
		dest      = newTestDestination("foo")
		task, err = NewTask(
			WithPG(pg),
			WithSource(tg),
			WithIntegration(dest.ig()),
			WithIntegrationFactory(dest.factory),
		)
	)
	diff.Test(t, t.Fatalf, err, nil)

	tg.add(0, hash(0), hash(0))
	tg.add(1, hash(2), hash(0))
	tg.add(2, hash(3), hash(2))

	dest.add(0, hash(0), hash(0))
	dest.add(1, hash(2), hash(0))

	diff.Test(t, t.Fatalf, nil, task.update(pg, 0, hash(0), 0, hash(0), 0, 0, 0))
	diff.Test(t, t.Fatalf, nil, task.update(pg, 1, hash(1), 0, hash(0), 0, 0, 0))

	diff.Test(t, t.Fatalf, task.Converge(), nil)
	diff.Test(t, t.Fatalf, task.Converge(), nil)
	diff.Test(t, t.Errorf, dest.blocks(), tg.blocks)
}

// A converge batch writes one task update for its last block. When that block
// is reorged out, the batch's earlier blocks have to go too -- no remaining
// update covers them, and converge requests them again on the next pass.
func TestConverge_ReorgBatchPrefix(t *testing.T) {
	var (
		pg        = testpg(t)
		tg        = &testGeth{}
		dest      = newTestDestination("foo")
		task, err = NewTask(
			WithPG(pg),
			WithConcurrency(1, 2),
			WithSource(tg),
			WithIntegration(dest.ig()),
			WithIntegrationFactory(dest.factory),
		)
	)
	diff.Test(t, t.Fatalf, err, nil)

	tg.add(0, hash(0), hash(0))
	tg.add(1, hash(1), hash(0))
	tg.add(2, hash(2), hash(1))
	dest.add(0, hash(0), hash(0))
	diff.Test(t, t.Fatalf, nil, task.update(pg, 0, hash(0), 0, hash(0), 0, 0, 0))

	// blocks 1 and 2 land in one batch, covered by a single update at 2
	diff.Test(t, t.Fatalf, task.Converge(), nil)
	checkQuery(t, pg, `select max(num) = 2 from shovel.task_updates`)

	// block 2 is reorged out. block 1 stays canonical, but the update that
	// covered it is deleted along with 2, so it must be reindexed.
	tg.blocks = tg.blocks[:2]
	tg.add(2, hash(22), hash(1))
	tg.add(3, hash(3), hash(22))

	diff.Test(t, t.Fatalf, task.Converge(), nil)
	diff.Test(t, t.Fatalf, task.Converge(), nil)
	diff.Test(t, t.Errorf, dest.blocks(), tg.blocks)
}

func TestConverge_DeltaBatchSize(t *testing.T) {
	const (
		batchSize   = 16
		concurrency = 2
	)
	var (
		pg        = testpg(t)
		tg        = &testGeth{}
		dest      = newTestDestination("foo")
		task, err = NewTask(
			WithPG(pg),
			WithConcurrency(concurrency, batchSize),
			WithSource(tg),
			WithRange(1, 0),
			WithIntegration(dest.ig()),
			WithIntegrationFactory(dest.factory),
		)
	)
	diff.Test(t, t.Fatalf, err, nil)

	tg.add(0, hash(0), hash(0))
	// reduce the batch size by 1 so that
	// the second part's limit is one less than it's part
	// adjusted batchSize
	for i := uint64(1); i <= batchSize-1; i++ {
		tg.add(i, hash(byte(i)), hash(byte(i-1)))
	}

	diff.Test(t, t.Errorf, nil, task.Converge())
	diff.Test(t, t.Errorf, dest.blocks(), tg.blocks[1:batchSize])
}

func TestConverge_MultipleTasks(t *testing.T) {
	var (
		tg          = &testGeth{}
		pg          = testpg(t)
		dest1       = newTestDestination("foo")
		dest2       = newTestDestination("bar")
		task1, err1 = NewTask(
			WithPG(pg),
			WithSource(tg),
			WithIntegration(dest1.ig()),
			WithIntegrationFactory(dest1.factory),
			WithRange(1, 0),
			WithConcurrency(1, 3),
		)
		task2, err2 = NewTask(
			WithPG(pg),
			WithSource(tg),
			WithIntegration(dest2.ig()),
			WithIntegrationFactory(dest2.factory),
			WithRange(1, 0),
			WithConcurrency(1, 3),
		)
	)
	diff.Test(t, t.Errorf, err1, nil)
	diff.Test(t, t.Errorf, err2, nil)

	tg.add(0, hash(0), hash(0))
	tg.add(1, hash(1), hash(0))
	tg.add(2, hash(2), hash(1))

	diff.Test(t, t.Errorf, task1.Converge(), nil)
	diff.Test(t, t.Errorf, dest1.blocks(), tg.blocks[1:])

	diff.Test(t, t.Errorf, len(dest2.blocks()), 0)
	diff.Test(t, t.Errorf, task2.Converge(), nil)
	diff.Test(t, t.Errorf, dest2.blocks(), tg.blocks[1:])
}

func TestConverge_LocalAhead(t *testing.T) {
	var (
		tg        = &testGeth{}
		pg        = testpg(t)
		dest      = newTestDestination("foo")
		task, err = NewTask(
			WithPG(pg),
			WithSource(tg),
			WithIntegration(dest.ig()),
			WithIntegrationFactory(dest.factory),
		)
	)
	diff.Test(t, t.Errorf, err, nil)
	tg.add(1, hash(1), hash(0))

	diff.Test(t, t.Fatalf, nil, task.update(pg, 0, hash(0), 0, hash(0), 0, 0, 0))
	diff.Test(t, t.Fatalf, nil, task.update(pg, 1, hash(1), 0, hash(0), 0, 0, 0))
	diff.Test(t, t.Fatalf, nil, task.update(pg, 2, hash(2), 0, hash(0), 0, 0, 0))

	diff.Test(t, t.Errorf, task.Converge(), ErrAhead)
}

func TestConverge_Done(t *testing.T) {
	var (
		pg        = testpg(t)
		tg        = &testGeth{}
		dest      = newTestDestination("foo")
		task, err = NewTask(
			WithPG(pg),
			WithSource(tg),
			WithIntegration(dest.ig()),
			WithIntegrationFactory(dest.factory),
			WithRange(1, 2),
		)
	)
	diff.Test(t, t.Fatalf, err, nil)

	tg.add(0, hash(0), hash(0))
	tg.add(1, hash(1), hash(0))
	tg.add(2, hash(2), hash(1))

	diff.Test(t, t.Errorf, nil, task.Converge())
	diff.Test(t, t.Errorf, nil, task.Converge())
	diff.Test(t, t.Errorf, ErrDone, task.Converge())
}

func TestPruneTask(t *testing.T) {
	pg := testpg(t)
	it := func(n uint8) {
		_, err := pg.Exec(context.Background(), `
			insert into shovel.task_updates(src_name, ig_name, num, hash)
			values ($1, $2, $3, $4)
		`, "foo", "bar", n, hash(n))
		if err != nil {
			t.Fatalf("inserting task: %d", n)
		}
	}
	for i := uint8(0); i < 10; i++ {
		it(i)
	}
	checkQuery(t, pg, `select count(*) = 10 from shovel.task_updates`)
	PruneTask(context.Background(), pg, 1)
	checkQuery(t, pg, `select count(*) = 1 from shovel.task_updates`)
}

func destFactory(dests ...*testDestination) func(config.Integration) (Destination, error) {
	return func(ig config.Integration) (Destination, error) {
		for i := range dests {
			if dests[i].name == ig.Name {
				return dests[i], nil
			}
		}
		return nil, fmt.Errorf("dest not found %s", ig.Name)
	}
}

func TestLoadTasks(t *testing.T) {
	ctx := context.Background()
	pqxtest.CreateDB(t, Schema)
	pg, err := pgxpool.New(ctx, pqxtest.DSNForTest(t))
	diff.Test(t, t.Fatalf, err, nil)

	conf := config.Root{
		PGURL: pqxtest.DSNForTest(t),
		Sources: []config.Source{
			config.Source{
				Name:    "foo",
				ChainID: 888,
				URLs:    []string{"http://foo"},
			},
		},
		Integrations: []config.Integration{
			config.Integration{
				Enabled: true,
				Name:    "bar",
				Table: wpg.Table{
					Name: "bar",
					Columns: []wpg.Column{
						wpg.Column{Name: "block_hash", Type: "bytea"},
					},
				},
				Block: []dig.BlockData{
					dig.BlockData{
						Name:   "block_hash",
						Column: "block_hash",
					},
				},
				Sources: []config.Source{
					config.Source{Name: "foo"},
				},
			},
		},
	}
	tasks, err := loadTasks(ctx, pg, conf)
	diff.Test(t, t.Fatalf, err, nil)
	diff.Test(t, t.Fatalf, len(tasks), 1)
	diff.Test(t, t.Fatalf, tasks[0].start, uint64(0))
}

func TestLatest(t *testing.T) {
	ctx := context.Background()
	pqxtest.CreateDB(t, Schema)
	pg, err := pgxpool.New(ctx, pqxtest.DSNForTest(t))
	diff.Test(t, t.Fatalf, err, nil)

	check := func(_ pgconn.CommandTag, err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	checkLatest := func(srcName string, want uint64) {
		var got uint64
		const q = `select num from shovel.latest where src_name = $1`
		if err := pg.QueryRow(ctx, q, srcName).Scan(&got); err != nil {
			t.Fatalf("checking %s error: %v", srcName, err)
		}
		if got != want {
			t.Errorf("wanted latest %d got %d", want, got)
		}
	}

	const q = `insert into shovel.task_updates (src_name, ig_name, num) values ($1, $2, $3)`
	check(pg.Exec(ctx, q, "main", "foo", 1))

	check(pg.Exec(ctx, q, "base", "foo", 1))
	check(pg.Exec(ctx, q, "base", "foo", 2))
	check(pg.Exec(ctx, q, "base", "bar", 1))
	checkLatest("main", 1)
	checkLatest("base", 1)

	check(pg.Exec(ctx, q, "base", "bar", 2))
	checkLatest("main", 1)
	checkLatest("base", 2)
}

func TestLatest_Backfill(t *testing.T) {
	ctx := context.Background()
	pqxtest.CreateDB(t, Schema)
	pg, err := pgxpool.New(ctx, pqxtest.DSNForTest(t))
	diff.Test(t, t.Fatalf, err, nil)

	check := func(_ pgconn.CommandTag, err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	checkLatest := func(srcName string, want uint64) {
		var got uint64
		const q = `select num from shovel.latest where src_name = $1`
		if err := pg.QueryRow(ctx, q, srcName).Scan(&got); err != nil {
			t.Fatalf("checking %s error: %v", srcName, err)
		}
		if got != want {
			t.Errorf("wanted latest %d got %d", want, got)
		}
	}

	const q = `insert into shovel.task_updates (src_name, ig_name, num) values ($1, $2, $3)`
	check(pg.Exec(ctx, q, "base", "foo", 100))
	check(pg.Exec(ctx, q, "base", "foo", 101))
	check(pg.Exec(ctx, q, "base", "bar", 1))
	checkLatest("base", 101)

	check(pg.Exec(ctx, q, "base", "bar", 91))
	checkLatest("base", 91)
}

func TestLatest_NoSrc(t *testing.T) {
	ctx := context.Background()
	pqxtest.CreateDB(t, Schema)
	pg, err := pgxpool.New(ctx, pqxtest.DSNForTest(t))
	diff.Test(t, t.Fatalf, err, nil)

	check := func(_ pgconn.CommandTag, err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	checkLatest := func(want uint64) {
		var got uint64
		const q = `select num from shovel.latest`
		if err := pg.QueryRow(ctx, q).Scan(&got); err != nil {
			t.Fatalf("checking table error: %v", err)
		}
		if got != want {
			t.Errorf("wanted latest %d got %d", want, got)
		}
	}

	const q = `insert into shovel.task_updates (src_name, ig_name, num) values ($1, $2, $3)`
	check(pg.Exec(ctx, q, "base", "foo", 100))
	check(pg.Exec(ctx, q, "base", "foo", 101))
	check(pg.Exec(ctx, q, "base", "bar", 1))
	checkLatest(101)
}
