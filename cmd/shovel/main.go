package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	npprof "net/http/pprof"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/indexsupply/shovel/shovel"
	"github.com/indexsupply/shovel/shovel/config"
	"github.com/indexsupply/shovel/shovel/web"
	"github.com/indexsupply/shovel/wctx"
	"github.com/indexsupply/shovel/wos"
	"github.com/indexsupply/shovel/wpg"
	"github.com/indexsupply/shovel/wslog"
)

func check(err error) {
	if err != nil {
		fmt.Printf("%s\n", err)
		os.Exit(1)
	}
}

func sqlfmt(s string) string {
	s = strings.ReplaceAll(s, "(", "(\n\t")
	s = strings.ReplaceAll(s, ")", "\n)")
	s = strings.ReplaceAll(s, ", ", ",\n\t")
	return s + ";"
}

func main() {
	var (
		ctx   = context.Background()
		cfile string

		printSchema bool
		skipMigrate bool
		listen      string
		notx        bool
		profile     string
		version     bool
		verbose     bool
		stallExit   time.Duration
	)
	flag.StringVar(&cfile, "config", "", "task config file")
	flag.BoolVar(&printSchema, "print-schema", false, "print schema and exit")
	flag.BoolVar(&skipMigrate, "skip-migrate", false, "do not run db migrations on startup")
	flag.StringVar(&listen, "l", "localhost:8546", "dashboard server listen address")
	flag.BoolVar(&notx, "notx", false, "disable pg tx")
	flag.StringVar(&profile, "profile", "", "run profile after indexing")
	flag.BoolVar(&version, "version", false, "version")
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.DurationVar(&stallExit, "stall-exit", 0,
		"exit when a task's watermark stalls this long while siblings advance (0 disables)")

	flag.Parse()

	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelInfo)
	if verbose {
		logLevel.Set(slog.LevelDebug)
	}

	lh := wslog.New(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	lh.RegisterContext(func(ctx context.Context) (string, any) {
		igName := wctx.IGName(ctx)
		if igName == "" {
			return "", nil
		}
		return "ig", igName
	})
	lh.RegisterContext(func(ctx context.Context) (string, any) {
		srcHost := wctx.SrcHost(ctx)
		if srcHost == "" {
			return "", nil
		}
		return "host", srcHost
	})
	lh.RegisterContext(func(ctx context.Context) (string, any) {
		srcName := wctx.SrcName(ctx)
		if srcName == "" {
			return "", nil
		}
		return "src", srcName
	})
	lh.RegisterContext(func(ctx context.Context) (string, any) {
		num, limit := wctx.NumLimit(ctx)
		if num == 0 || limit == 0 {
			return "", nil
		}
		return "req", fmt.Sprintf("%d/%d", num, limit)
	})
	slog.SetDefault(slog.New(lh.WithAttrs([]slog.Attr{
		slog.String("v", Commit),
	})))

	ctx = wctx.WithVersion(ctx, Commit)

	if version {
		fmt.Printf("v%s %s\n", Version, Commit)
		os.Exit(0)
	}

	var (
		conf  config.Root
		pgurl string
	)
	switch {
	case cfile == "":
		pgurl = os.Getenv("DATABASE_URL")
	case cfile != "":
		f, err := os.Open(cfile)
		check(err)
		check(json.NewDecoder(f).Decode(&conf))
		check(config.ValidateFix(&conf))
		pgurl = wos.Getenv(conf.PGURL)
	}

	if printSchema {
		for _, stmt := range config.DDL(conf) {
			fmt.Printf("%s\n", sqlfmt(stmt))
		}
		os.Exit(0)
	}

	pg, err := wpg.NewPool(ctx, pgurl)
	check(err)

	if !skipMigrate {
		dbtx, err := pg.Begin(ctx)
		check(err)
		_, err = dbtx.Exec(
			ctx,
			"select pg_advisory_xact_lock($1)",
			wpg.LockHash("main.migrate"),
		)
		check(err)
		_, err = dbtx.Exec(ctx, shovel.Schema)
		check(err)
		check(config.Migrate(ctx, dbtx, conf))
		check(dbtx.Commit(ctx))
	}

	var (
		pbuf bytes.Buffer
		mgr  = shovel.NewManager(ctx, pg, conf)
		wh   = web.New(mgr, &conf, pg)
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/", wh.Index)
	mux.HandleFunc("/diag", wh.Diag)
	mux.HandleFunc("/metrics", wh.Prom)
	mux.HandleFunc("/login", wh.Login)
	mux.Handle("/task-updates", wh.Authn(wh.Updates))
	mux.Handle("/add-source", wh.Authn(wh.AddSource))
	mux.Handle("/save-source", wh.Authn(wh.SaveSource))
	mux.Handle("/add-integration", wh.Authn(wh.AddIntegration))
	mux.Handle("/save-integration", wh.Authn(wh.SaveIntegration))
	mux.HandleFunc("/debug/pprof/", npprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", npprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", npprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", npprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", npprof.Trace)
	mux.HandleFunc("/debug/pprof/capture", func(w http.ResponseWriter, r *http.Request) {
		w.Write(pbuf.Bytes())
	})
	go http.ListenAndServe(listen, log(true, mux))

	if profile == "cpu" {
		check(pprof.StartCPUProfile(&pbuf))
	}

	go func() {
		check(wh.PushUpdates())
	}()

	go func() {
		for {
			check(shovel.PruneTask(ctx, pg, 200))
			time.Sleep(time.Minute * 10)
		}
	}()

	if stallExit > 0 {
		go mgr.WatchStalls(ctx, time.Minute, stallExit, func(stalled []string) {
			slog.ErrorContext(ctx, "task-stalled",
				"tasks", strings.Join(stalled, ","),
				"stall_exit", stallExit.String(),
			)
			// The frozen goroutine's stack is the diagnosis; capture it before
			// the supervisor replaces the process.
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
			os.Exit(70)
		})
	}

	ec := make(chan error)
	go mgr.Run(ec)
	if err := <-ec; err != nil {
		fmt.Printf("startup error: %s\n", err)
		os.Exit(1)
	}

	switch profile {
	case "cpu":
		pprof.StopCPUProfile()
	case "heap":
		check(pprof.Lookup("heap").WriteTo(&pbuf, 0))
	}
	select {}
}

func log(v bool, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		h.ServeHTTP(w, r) // serve the original request
		if v {
			slog.Info("", "e", time.Since(t0), "u", r.URL.Path)
		}
	})
}

// Set using: go build -ldflags="-X main.Version=XXX"
var (
	Version string
	Commit  = func() string {
		bi, ok := debug.ReadBuildInfo()
		if !ok {
			return "ernobuildinfo"
		}
		var (
			revision = ""
			modified bool
		)
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				revision = s.Value[:4]
			case "vcs.modified":
				modified = s.Value == "true"
			}
		}
		if !modified {
			return revision
		}
		return revision + "-"
	}()
)
