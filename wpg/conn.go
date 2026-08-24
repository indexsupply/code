package wpg

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Conn interface {
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	conf, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	// 5s kills legitimate bulk inserts: startup and busy-box converge statements
	// routinely exceed it, and the startup form is fatal (crash loop until one
	// start gets through). An explicit runtime param also beats PGOPTIONS, so the
	// deployment cannot override it from the outside. 60s bounds a genuinely hung
	// statement while clearing every insert observed in production (worst ~4.2s).
	conf.ConnConfig.RuntimeParams["statement_timeout"] = "60s"
	conf.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "10s"
	return pgxpool.NewWithConfig(context.Background(), conf)
}

var (
	lockCollisions    = map[int64]string{}
	lockCollisionsMut sync.Mutex
)

// Uses fnva to compute a hash
// This is an expensive function since it uses a global map
// and a mutex to check if there was a hash collision.
func LockHash(s string) int64 {
	f := fnv.New32a()
	if _, err := f.Write([]byte(s)); err != nil {
		panic(err)
	}
	n := int64(f.Sum32())

	lockCollisionsMut.Lock()
	defer lockCollisionsMut.Unlock()
	if prev, ok := lockCollisions[n]; ok {
		if prev != s {
			panic(fmt.Sprintf("fnva collision: %s %s %d", s, prev, n))
		}
	} else {
		lockCollisions[n] = s
	}
	return n
}
