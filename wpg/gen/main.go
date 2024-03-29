package main

import (
	"bytes"
	"context"
	"database/sql"
	"go/format"
	"html/template"
	"os"
	"time"

	"blake.io/pqx/pqxtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

const wordsTemplate = `
// Code generated by wpg/gen/main.go; DO NOT EDIT.

package wpg

var reservedWords = map[string]struct{}{
	{{ range . -}}
	"{{ . -}}": {},
	{{ end -}}
}
`

func main() {
	ctx := context.Background()
	sql.Register("postgres", stdlib.GetDefaultDriver())
	pqxtest.Start(time.Second*30, 0)
	pg, err := pgxpool.New(ctx, pqxtest.DSN())
	check(err)
	const q = `select word from pg_get_keywords() where catdesc = 'reserved'`
	rows, _ := pg.Query(ctx, q)
	words, err := pgx.CollectRows(rows, pgx.RowTo[string])
	check(err)
	pqxtest.Shutdown()

	b := new(bytes.Buffer)
	t := template.Must(template.New("_").Parse(wordsTemplate))
	check(t.Execute(b, words))
	code, err := format.Source(b.Bytes())
	check(err)
	check(os.WriteFile("./reserved_words.go", code, 0644))
}
