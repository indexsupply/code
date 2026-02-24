// JSON RPC client
package jrpc2

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/holiman/uint256"
	"github.com/indexsupply/shovel/eth"
	"github.com/indexsupply/shovel/shovel/glf"
	"github.com/indexsupply/shovel/wctx"

	"github.com/goccy/go-json"
	"github.com/klauspost/compress/gzhttp"
	"golang.org/x/sync/errgroup"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type URL struct {
	parsed   *url.URL
	provided string
}

func MustURL(provided string) *URL {
	parsed, err := url.Parse(provided)
	if err != nil {
		fmt.Printf("unable to parse url: %s\n", provided)
		os.Exit(1)
	}
	return &URL{parsed: parsed, provided: provided}
}

func (u *URL) Hostname() string {
	return u.parsed.Hostname()
}

func (u *URL) String() string {
	return u.parsed.String()
}

func randbytes() []byte {
	b := make([]byte, 10)
	rand.Read(b)
	return b
}

func New(providedURLs ...string) *Client {
	var (
		urls           []*URL
		debug, nocache bool
	)
	for _, provided := range providedURLs {
		debug = strings.Contains(provided, "debug")
		nocache = strings.Contains(provided, "nocache")
		urls = append(urls, MustURL(provided))
	}
	return &Client{
		d:       debug,
		nocache: nocache,
		hc: &http.Client{
			Timeout:   10 * time.Second,
			Transport: gzhttp.Transport(http.DefaultTransport),
		},
		urls:         urls,
		pollDuration: time.Second,
		lcache:       NumHash{maxreads: 20},
		bcache:       cache{maxreads: 20},
		hcache:       cache{maxreads: 20},
	}
}

type Client struct {
	nocache bool
	d       bool
	hc      *http.Client
	urls    []*URL
	wsurl   string

	reqCounter   uint64
	pollDuration time.Duration

	lcache NumHash
	bcache cache
	hcache cache
}

func (c *Client) NextURL() *URL {
	atomic.AddUint64(&c.reqCounter, 1)
	next := c.reqCounter % uint64(len(c.urls))
	return c.urls[next]
}

func (c *Client) NumURLs() int {
	return len(c.urls)
}

func (c *Client) WithMaxReads(n int) *Client {
	c.lcache.maxreads = n
	c.bcache.maxreads = n
	c.hcache.maxreads = n
	return c
}

func (c *Client) WithPollDuration(d time.Duration) *Client {
	c.pollDuration = d
	return c
}

func (c *Client) WithWSURL(url string) *Client {
	c.wsurl = url
	return c
}

func (c *Client) debug(r io.Reader) io.Reader {
	if !c.d {
		return r
	}
	return io.TeeReader(r, os.Stdout)
}

type request struct {
	ID      string `json:"id"`
	Version string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

func (c *Client) do(ctx context.Context, url string, dest, req any) error {
	var (
		eg   errgroup.Group
		r, w = io.Pipe()
		resp *http.Response
	)
	eg.Go(func() error {
		defer w.Close()
		return json.NewEncoder(w).Encode(req)
	})
	eg.Go(func() error {
		req, err := http.NewRequest("POST", url, c.debug(r))
		if err != nil {
			return fmt.Errorf("unable to new request: %w", err)
		}
		req.Header.Add("content-type", "application/json")
		resp, err = c.hc.Do(req)
		if err != nil {
			return fmt.Errorf("unable to do http request: %w", err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		text := strings.Map(func(r rune) rune {
			if unicode.IsPrint(r) {
				return r
			}
			return -1
		}, string(b))
		const msg = "rpc http error: %d %.100s"
		return fmt.Errorf(msg, resp.StatusCode, text)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(c.debug(resp.Body)).Decode(dest); err != nil {
		return fmt.Errorf("unable to json decode: %w", err)
	}
	wctx.CounterAdd(ctx, 1)
	return nil
}

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e Error) Exists() bool {
	return e.Code != 0
}

func (e Error) Error() string {
	return fmt.Sprintf("code=%d msg=%s", e.Code, e.Message)
}

type NumHash struct {
	sync.Mutex
	err      error
	once     sync.Once
	maxreads int
	nreads   int
	Num      eth.Uint64 `json:"number"`
	Hash     eth.Bytes  `json:"hash"`
}

func (nh *NumHash) error(err error) {
	nh.Lock()
	nh.nreads = 0
	nh.err = err
	nh.Unlock()
}

func (nh *NumHash) update(n eth.Uint64, h []byte) {
	nh.Lock()
	defer nh.Unlock()
	if n <= nh.Num {
		return
	}
	nh.nreads = 0
	nh.Num = n
	nh.Hash.Write(h)
}

func (nh *NumHash) get(ctx context.Context, n uint64) (uint64, []byte, bool) {
	nh.Lock()
	defer nh.Unlock()

	if err := nh.err; err != nil {
		switch {
		case errors.Is(err, net.ErrClosed), errors.Is(err, context.DeadlineExceeded):
			slog.DebugContext(ctx, "rpc connection reset")
		default:
			slog.DebugContext(ctx, "rpc connection error", "error", err)
		}
		nh.err = nil
		nh.once = sync.Once{}
		return 0, nil, false
	}

	if n == 0 || uint64(nh.Num) < n {
		slog.DebugContext(ctx, "latest cache miss", "n", n, "latest", nh.Num)
		return 0, nil, false
	}

	if nh.nreads >= nh.maxreads {
		slog.DebugContext(ctx, "expiring latest cache",
			"n", n,
			"latest", nh.Num,
			"nreads", nh.nreads,
			"maxreads", nh.maxreads,
		)
		nh.nreads = 0
		nh.Num = eth.Uint64(0)
		nh.Hash.Write([]byte{})
		return 0, nil, false
	}

	nh.nreads++
	slog.DebugContext(ctx, "latest cache hit",
		"n", n,
		"latest", nh.Num,
		"nreads", nh.nreads,
	)
	h := make([]byte, 32)
	copy(h, nh.Hash)
	return uint64(nh.Num), h, true
}

func (c *Client) wsListen(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	wsc, _, err := websocket.Dial(ctx, c.wsurl, nil)
	if err != nil {
		c.lcache.error(fmt.Errorf("ws dial %q: %w", c.wsurl, err))
		return
	}
	err = wsjson.Write(ctx, wsc, request{
		ID:      "1",
		Version: "2.0",
		Method:  "eth_subscribe",
		Params:  []any{"newHeads"},
	})
	if err != nil {
		c.lcache.error(fmt.Errorf("ws write %q: %w", c.wsurl, err))
		return
	}
	res := struct {
		Error `json:"error"`
		P     struct {
			R NumHash `json:"result"`
		} `json:"params"`
	}{}
	for {
		if err := wsjson.Read(ctx, wsc, &res); err != nil {
			c.lcache.error(fmt.Errorf("ws read %q: %w", c.wsurl, err))
			return
		}
		slog.DebugContext(ctx, "websocket newHeads",
			"n", res.P.R.Num,
			"h", fmt.Sprintf("%.4x", res.P.R.Hash),
		)
		c.lcache.update(res.P.R.Num, res.P.R.Hash)
	}
}

func (c *Client) httpPoll(ctx context.Context, url string) {
	var (
		ticker = time.NewTicker(c.pollDuration)
		hresp  = headerResp{}
	)
	defer ticker.Stop()
	for range ticker.C {
		err := c.do(ctx, url, &hresp, request{
			ID:      "1",
			Version: "2.0",
			Method:  "eth_getBlockByNumber",
			Params:  []any{"latest", false},
		})
		if err != nil {
			c.lcache.error(err)
			return
		}
		if hresp.Error.Exists() {
			const tag = "eth_getBlockByNumber/latest"
			c.lcache.error(fmt.Errorf("rpc=%s %w", tag, hresp.Error))
			return
		}
		slog.DebugContext(ctx, "http poll",
			"n", hresp.Number,
			"h", fmt.Sprintf("%.4x", hresp.Hash),
		)
		c.lcache.update(hresp.Number, hresp.Hash)
	}
}

// Returns the latest block number/hash greater than n.
// If n is lower than the cached block number,
// returns the cached value; otherwise, fetches the
// latest block. Caching is based on comparing n
// with the cached block number, not on time/LRU methods.
//
// When n is 0, Latest always fetches the latest block
// rather than using the cached value,
// bypassing the caching mechanism.
func (c *Client) Latest(ctx context.Context, url string, n uint64) (uint64, []byte, error) {
	c.lcache.once.Do(func() {
		switch {
		case len(c.wsurl) > 0:
			slog.DebugContext(ctx, "jrpc2 ws listening")
			go c.wsListen(context.Background())
		default:
			slog.DebugContext(ctx, "jrpc2 http polling")
			go c.httpPoll(context.Background(), url)
		}
	})
	if n, h, ok := c.lcache.get(ctx, n); ok {
		return n, h, nil
	}

	hresp := headerResp{}
	err := c.do(ctx, url, &hresp, request{
		ID:      fmt.Sprintf("latest-%d-%x", n, randbytes()),
		Version: "2.0",
		Method:  "eth_getBlockByNumber",
		Params:  []any{"latest", false},
	})
	if err != nil {
		return 0, nil, fmt.Errorf("unable request latest: %w", err)
	}
	if hresp.Error.Exists() {
		const tag = "eth_getBlockByNumber/latest"
		return 0, nil, fmt.Errorf("rpc=%s %w", tag, hresp.Error)
	}
	slog.DebugContext(ctx, "http-get-latest",
		"n", hresp.Number,
		"h", fmt.Sprintf("%.4x", hresp.Hash),
	)
	c.lcache.update(hresp.Number, hresp.Hash)
	return uint64(hresp.Number), hresp.Hash, nil
}

func (c *Client) Hash(ctx context.Context, url string, n uint64) ([]byte, error) {
	hresp := headerResp{}
	err := c.do(ctx, url, &hresp, request{
		ID:      fmt.Sprintf("hash-%d-%x", n, randbytes()),
		Version: "2.0",
		Method:  "eth_getBlockByNumber",
		Params:  []any{"0x" + strconv.FormatUint(n, 16), true},
	})
	if err != nil {
		return nil, fmt.Errorf("unable request hash: %w", err)
	}
	if hresp.Error.Exists() {
		const tag = "eth_getBlockByNumber/hash"
		return nil, fmt.Errorf("rpc=%s %w", tag, hresp.Error)
	}
	return hresp.Hash, nil
}

type key struct {
	a, b uint64
}

type blockmap map[uint64]*eth.Block

func (c *Client) Get(
	ctx context.Context,
	url string,
	filter *glf.Filter,
	start, limit uint64,
) ([]eth.Block, error) {
	t0 := time.Now()
	defer func() {
		slog.DebugContext(ctx,
			"jrpc2-get",
			"filter", filter,
			"elapsed", time.Since(t0),
		)
	}()
	var (
		blocks []eth.Block
		err    error
	)
	switch {
	case filter.UseBlocks:
		blocks, err = c.bcache.get(c.nocache, ctx, url, start, limit, c.blocks)
		if err != nil {
			return nil, fmt.Errorf("getting blocks: %w", err)
		}
	case filter.UseHeaders:
		blocks, err = c.hcache.get(c.nocache, ctx, url, start, limit, c.headers)
		if err != nil {
			return nil, fmt.Errorf("getting headers: %w", err)
		}
	default:
		for i := uint64(0); i < limit; i++ {
			blocks = append(blocks, eth.Block{
				Header: eth.Header{
					Number: eth.Uint64(start + i),
				},
			})
		}
	}

	bm := make(blockmap)
	for i := range blocks {
		bm[blocks[i].Num()] = &blocks[i]
	}

	switch {
	case filter.UseReceipts:
		if err := c.receipts(ctx, url, bm, start, limit); err != nil {
			return nil, fmt.Errorf("getting receipts: %w", err)
		}
	case filter.UseLogs:
		if err := c.logs(ctx, url, filter, bm, start, limit); err != nil {
			return nil, fmt.Errorf("getting logs: %w", err)
		}
	case filter.UseTraces:
		if err := c.traces(ctx, url, bm, start, limit); err != nil {
			return nil, fmt.Errorf("getting traces: %w", err)
		}
	}
	return blocks, nil
}

type blockResp struct {
	Error      `json:"error"`
	*eth.Block `json:"result"`
}

type segment struct {
	sync.Mutex
	nreads int
	done   bool
	d      []eth.Block
}

type cache struct {
	sync.Mutex
	maxreads int
	segments map[key]*segment
}

type getter func(ctx context.Context, url string, start, limit uint64) ([]eth.Block, error)

func (c *cache) pruneMaxRead() {
	for k, v := range c.segments {
		v.Lock()
		if v.nreads >= c.maxreads {
			delete(c.segments, k)
		}
		v.Unlock()
	}
}

func (c *cache) pruneSegments() {
	const size = 5
	if len(c.segments) <= size {
		return
	}
	var keys []key
	for k := range c.segments {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].a > keys[j].a
	})
	for i := range keys[size:] {
		delete(c.segments, keys[size+i])
	}
}

func (c *cache) get(nocache bool, ctx context.Context, url string, start, limit uint64, f getter) ([]eth.Block, error) {
	if nocache {
		return f(ctx, url, start, limit)
	}
	c.Lock()
	if c.segments == nil {
		c.segments = make(map[key]*segment)
	}
	c.pruneMaxRead()
	seg, ok := c.segments[key{start, limit}]
	if !ok {
		seg = &segment{}
		c.segments[key{start, limit}] = seg
	}
	c.pruneSegments()
	c.Unlock()

	seg.Lock()
	defer seg.Unlock()
	seg.nreads++
	if seg.done {
		return seg.d, nil
	}

	blocks, err := f(ctx, url, start, limit)
	if err != nil {
		return nil, fmt.Errorf("cache get: %w", err)
	}

	seg.d = blocks
	seg.done = true
	return seg.d, nil
}

func (c *Client) blocks(ctx context.Context, url string, start, limit uint64) ([]eth.Block, error) {
	var (
		t0     = time.Now()
		reqs   = make([]request, limit)
		resps  = make([]blockResp, limit)
		blocks = make([]eth.Block, limit)
	)
	for i := uint64(0); i < limit; i++ {
		reqs[i] = request{
			ID:      fmt.Sprintf("blocks-%d-%d-%x", start, limit, randbytes()),
			Version: "2.0",
			Method:  "eth_getBlockByNumber",
			Params:  []any{eth.EncodeUint64(start + i), true},
		}
		resps[i].Block = &blocks[i]
	}
	err := c.do(ctx, url, &resps, reqs)
	if err != nil {
		return nil, fmt.Errorf("requesting blocks: %w", err)
	}
	for i := range resps {
		if resps[i].Error.Exists() {
			const tag = "eth_getBlockByNumber"
			return nil, fmt.Errorf("rpc=%s %w", tag, resps[i].Error)
		}
	}
	slog.DebugContext(ctx, "http-get-blocks", "elapsed", time.Since(t0))
	return blocks, validate("blocks", start, limit, blocks)
}

func validate(caller string, start, limit uint64, blocks []eth.Block) error {
	if len(blocks) == 0 {
		return fmt.Errorf("%s: no blocks", caller)
	}
	first, last := blocks[0].Num(), blocks[len(blocks)-1].Num()
	if uint64(first) != start {
		const tag = "%s: rpc response contains invalid data. requested first: %d got: %d"
		return fmt.Errorf(tag, caller, start, first)
	}
	if uint64(last) != start+limit-1 {
		const tag = "%s: rpc response contains invalid data. requested last: %d got: %d"
		return fmt.Errorf(tag, caller, start+limit-1, last)
	}
	for i := 1; i < len(blocks); i++ {
		prev, curr := blocks[i-1], blocks[i]
		if !bytes.Equal(curr.Header.Parent, prev.Hash()) {
			slog.Error("rpc response contains invalid data",
				"num", prev.Num(),
				"hash", fmt.Sprintf("%.4x", prev.Header.Hash),
				"next-num", curr.Num(),
				"next-parent", fmt.Sprintf("%.4x", curr.Header.Parent),
				"next-hash", fmt.Sprintf("%.4x", curr.Header.Hash),
			)
			return fmt.Errorf("%s: corrupt chain segment", caller)
		}
	}
	return nil
}

type headerResp struct {
	Error       `json:"error"`
	*eth.Header `json:"result"`
}

func (c *Client) headers(ctx context.Context, url string, start, limit uint64) ([]eth.Block, error) {
	var (
		t0     = time.Now()
		reqs   = make([]request, limit)
		resps  = make([]headerResp, limit)
		blocks = make([]eth.Block, limit)
	)
	for i := uint64(0); i < limit; i++ {
		reqs[i] = request{
			ID:      fmt.Sprintf("headers-%d-%d-%x", start, limit, randbytes()),
			Version: "2.0",
			Method:  "eth_getBlockByNumber",
			Params:  []any{eth.EncodeUint64(start + i), false},
		}
		resps[i].Header = &blocks[i].Header
	}
	err := c.do(ctx, url, &resps, reqs)
	if err != nil {
		return nil, fmt.Errorf("requesting headers: %w", err)
	}
	for i := range resps {
		if resps[i].Error.Exists() {
			const tag = "eth_getBlockByNumber/headers"
			return nil, fmt.Errorf("rpc=%s %w", tag, resps[i].Error)
		}
	}
	slog.DebugContext(ctx, "http-get-headers", "elapsed", time.Since(t0))
	return blocks, validate("headers", start, limit, blocks)
}

type receiptResult struct {
	BlockHash         eth.Bytes   `json:"blockHash"`
	BlockNum          eth.Uint64  `json:"blockNumber"`
	TxHash            eth.Bytes   `json:"transactionHash"`
	TxIdx             eth.Uint64  `json:"transactionIndex"`
	TxType            eth.Byte    `json:"type"`
	TxFrom            eth.Bytes   `json:"from"`
	TxTo              eth.Bytes   `json:"to"`
	Status            eth.Byte    `json:"status"`
	GasUsed           eth.Uint64  `json:"gasUsed"`
	EffectiveGasPrice uint256.Int `json:"effectiveGasPrice"`
	Logs              eth.Logs    `json:"logs"`
	ContractAddress   eth.Bytes   `json:"contractAddress"`
	L1BaseFeeScalar     *uint256.Int `json:"l1BaseFeeScalar,omitempty"`
	L1BlobBaseFee       *uint256.Int `json:"l1BlobBaseFee,omitempty"`
	L1BlobBaseFeeScalar *uint256.Int `json:"l1BlobBaseFeeScalar,omitempty"`
	L1Fee               *uint256.Int `json:"l1Fee,omitempty"`
	L1GasPrice          *uint256.Int `json:"l1GasPrice,omitempty"`
	L1GasUsed           *eth.Uint64  `json:"l1GasUsed,omitempty"`
}

type receiptResp struct {
	Error  `json:"error"`
	Result []receiptResult `json:"result"`
}

func (c *Client) receipts(ctx context.Context, url string, bm blockmap, start, limit uint64) error {
	var (
		reqs  = make([]request, limit)
		resps = make([]receiptResp, limit)
	)
	for i := uint64(0); i < limit; i++ {
		reqs[i] = request{
			ID:      fmt.Sprintf("receipts-%d-%d-%x", start, limit, randbytes()),
			Version: "2.0",
			Method:  "eth_getBlockReceipts",
			Params:  []any{eth.EncodeUint64(start + i)},
		}
	}
	err := c.do(ctx, url, &resps, reqs)
	if err != nil {
		return fmt.Errorf("requesting receipts: %w", err)
	}
	for i := range resps {
		if resps[i].Error.Exists() {
			const tag = "eth_getBlockReceipts"
			return fmt.Errorf("rpc=%s %w", tag, resps[i].Error)
		}
	}
	for i := range resps {
		if len(resps[i].Result) == 0 {
			slog.ErrorContext(ctx, "no rpc error but empty result")
			continue
		}
		blockNum := uint64(resps[i].Result[0].BlockNum)
		if blockNum < start || blockNum > start+limit {
			const tag = "eth_getBlockReceipts out of range block. num=%d start=%d lim=%d"
			return fmt.Errorf(tag, blockNum, start, limit)
		}
		b, ok := bm[blockNum]
		if !ok {
			return fmt.Errorf("block not found")
		}
		b.Header.Hash.Write(resps[i].Result[0].BlockHash)
		for j := range resps[i].Result {
			tx := b.Tx(uint64(resps[i].Result[j].TxIdx))
			tx.PrecompHash.Write(resps[i].Result[j].TxHash)
			tx.Type.Write(byte(resps[i].Result[j].TxType))
			tx.From.Write(resps[i].Result[j].TxFrom)
			tx.To.Write(resps[i].Result[j].TxTo)
			tx.Status.Write(byte(resps[i].Result[j].Status))
			tx.GasUsed = resps[i].Result[j].GasUsed
			tx.EffectiveGasPrice = resps[i].Result[j].EffectiveGasPrice
			tx.Logs = make([]eth.Log, len(resps[i].Result[j].Logs))
			tx.ContractAddress.Write(resps[i].Result[j].ContractAddress)
			copy(tx.Logs, resps[i].Result[j].Logs)
			tx.L1BaseFeeScalar = resps[i].Result[j].L1BaseFeeScalar
			tx.L1BlobBaseFee = resps[i].Result[j].L1BlobBaseFee
			tx.L1BlobBaseFeeScalar = resps[i].Result[j].L1BlobBaseFeeScalar
			tx.L1Fee = resps[i].Result[j].L1Fee
			tx.L1GasPrice = resps[i].Result[j].L1GasPrice
			tx.L1GasUsed = resps[i].Result[j].L1GasUsed
		}
	}
	return nil
}

type logResult struct {
	*eth.Log
	BlockHash eth.Bytes  `json:"blockHash"`
	BlockNum  eth.Uint64 `json:"blockNumber"`
	TxHash    eth.Bytes  `json:"transactionHash"`
	TxIdx     eth.Uint64 `json:"transactionIndex"`
	Removed   bool       `json:"removed"`
}

type logResp struct {
	Error  `json:"error"`
	Result []logResult `json:"result"`
}

func (c *Client) logs(ctx context.Context, url string, filter *glf.Filter, bm blockmap, start, limit uint64) error {
	var (
		t0        = time.Now()
		fromBlock = start
		toBlock   = start + limit - 1
		lf        = struct {
			From    string     `json:"fromBlock"`
			To      string     `json:"toBlock"`
			Address []string   `json:"address"`
			Topics  [][]string `json:"topics"`
		}{
			From:    eth.EncodeUint64(fromBlock),
			To:      eth.EncodeUint64(toBlock),
			Address: filter.Addresses(),
			Topics:  filter.Topics(),
		}
		resp = []any{
			&headerResp{},
			&logResp{},
		}
	)
	err := c.do(ctx, url, &resp, []request{
		request{
			ID:      fmt.Sprintf("blocks-%d-%d-%x", start, limit, randbytes()),
			Version: "2.0",
			Method:  "eth_getBlockByNumber",
			Params:  []any{lf.To, false},
		},
		request{
			ID:      fmt.Sprintf("logs-%d-%d-%x", start, limit, randbytes()),
			Version: "2.0",
			Method:  "eth_getLogs",
			Params:  []any{lf},
		},
	})
	if err != nil {
		return fmt.Errorf("making logs request: %w", err)
	}
	var (
		hresp = resp[0].(*headerResp)
		lresp = resp[1].(*logResp)
	)
	switch {
	case hresp.Error.Exists():
		return fmt.Errorf("rpc=eth_getLogs/eth_getBlockByNumber %w", lresp.Error)
	case lresp.Error.Exists():
		return fmt.Errorf("rpc=eth_getLogs %w", lresp.Error)
	case hresp.Header == nil:
		return fmt.Errorf("eth backend missing logs for block: %d", toBlock)
	}
	var logsByTx = map[key][]logResult{}
	for i := range lresp.Result {
		var (
			blockNum = uint64(lresp.Result[i].BlockNum)
			txIdx    = uint64(lresp.Result[i].TxIdx)
			k        = key{blockNum, txIdx}
		)
		if blockNum < start || blockNum >= start+limit {
			const tag = "eth_getLogs out of range block. num=%d start=%d lim=%d"
			return fmt.Errorf(tag, blockNum, start, limit)
		}
		if logs, ok := logsByTx[k]; ok {
			logsByTx[k] = append(logs, lresp.Result[i])
			continue
		}
		logsByTx[k] = []logResult{lresp.Result[i]}
	}

	for k, logs := range logsByTx {
		b, ok := bm[k.a]
		if !ok {
			return fmt.Errorf("block not found")
		}
		b.Lock()
		b.Header.Hash.Write(logs[0].BlockHash)
		tx := b.Tx(k.b)
		tx.PrecompHash.Write(logs[0].TxHash)
		for i := range logs {
			tx.Logs.Add(logs[i].Log)
		}
		b.Unlock()
	}
	slog.DebugContext(ctx, "http-get-logs",
		"nlogs", len(lresp.Result),
		"elapsed", time.Since(t0),
	)
	return nil
}

type traceBlockResult struct {
	BlockHash eth.Bytes       `json:"blockHash"`
	BlockNum  uint64          `json:"blockNumber"`
	TxHash    eth.Bytes       `json:"transactionHash"`
	TxIdx     uint64          `json:"transactionPosition"`
	Action    eth.TraceAction `json:"action"`
}

type traceBlockResp struct {
	Error  `json:"error"`
	Result []traceBlockResult `json:"result"`
}

func (c *Client) traces(ctx context.Context, url string, bm blockmap, start, limit uint64) error {
	t0 := time.Now()
	for i := uint64(0); i < limit; i++ {
		res := traceBlockResp{}
		req := request{
			ID:      fmt.Sprintf("traces-%d-%d-%x", start, limit, randbytes()),
			Version: "2.0",
			Method:  "trace_block",
			Params:  []any{eth.EncodeUint64(start + i)},
		}
		err := c.do(ctx, url, &res, req)
		if err != nil {
			return fmt.Errorf("requesting traces: %w", err)
		}
		if res.Error.Exists() {
			const tag = "trace_block"
			return fmt.Errorf("rpc=%s %w", tag, res.Error)
		}
		if len(res.Result) == 0 {
			return fmt.Errorf("no rpc error but empty result")
		}
		block, ok := bm[res.Result[0].BlockNum]
		if !ok {
			return fmt.Errorf("missing block in block map")
		}
		block.Header.Hash.Write(res.Result[0].BlockHash)

		var tracesByTx = map[key][]traceBlockResult{}
		for i := range res.Result {
			k := key{block.Num(), uint64(res.Result[i].TxIdx)}
			if traces, ok := tracesByTx[k]; ok {
				tracesByTx[k] = append(traces, res.Result[i])
				continue
			}
			tracesByTx[k] = []traceBlockResult{res.Result[i]}
		}
		for k, traces := range tracesByTx {
			tx := block.Tx(k.b)
			tx.PrecompHash.Write(traces[0].TxHash)
			tx.TraceActions = make([]eth.TraceAction, len(traces))
			for i := range traces {
				ta := traces[i].Action
				ta.Idx = uint64(i)
				tx.TraceActions[i] = ta
			}
		}
	}
	slog.DebugContext(ctx, "http-get-traces", "elapsed", time.Since(t0))
	return nil
}
