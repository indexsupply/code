// JSON RPC client
package jrpc2

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/indexsupply/x/eth"
	"github.com/indexsupply/x/shovel/glf"

	"github.com/goccy/go-json"
	"github.com/klauspost/compress/gzhttp"
	"golang.org/x/sync/errgroup"
)

func New(url string) *Client {
	return &Client{
		d: strings.Contains(url, "debug"),
		hc: &http.Client{
			Transport: gzhttp.Transport(http.DefaultTransport),
		},
		url:    url,
		bcache: make(blockCache, 400),
		hcache: make(blockCache, 400),
	}
}

type Client struct {
	d   bool
	hc  *http.Client
	url string

	cacheMut sync.Mutex
	bcache   blockCache
	hcache   blockCache
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

func (c *Client) do(dest, req any) error {
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
		req, err := http.NewRequest("POST", c.url, c.debug(r))
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

func (c *Client) Latest() (uint64, []byte, error) {
	hresp := headerResp{}
	err := c.do(&hresp, request{
		ID:      "1",
		Version: "2.0",
		Method:  "eth_getBlockByNumber",
		Params:  []any{"latest", false},
	})
	if err != nil {
		return 0, nil, fmt.Errorf("unable request hash: %w", err)
	}
	if hresp.Error.Exists() {
		const tag = "eth_getBlockByNumber/latest"
		return 0, nil, fmt.Errorf("rpc=%s %w", tag, hresp.Error)
	}
	return uint64(hresp.Number), hresp.Hash, nil
}

func (c *Client) Hash(n uint64) ([]byte, error) {
	hresp := headerResp{}
	err := c.do(&hresp, request{
		ID:      "1",
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
	b uint64
	t uint64
}

type (
	blockmap map[uint64]*eth.Block
	txmap    map[key]*eth.Tx
)

func (c *Client) Get(filter *glf.Filter, start, limit uint64) ([]eth.Block, error) {
	var (
		blocks []eth.Block
		err    error
	)
	switch {
	case filter.UseBlocks:
		blocks, err = c.blocks2(start, limit)
		if err != nil {
			return nil, fmt.Errorf("getting blocks: %w", err)
		}
	case filter.UseHeaders:
		blocks, err = c.headers2(start, limit)
		if err != nil {
			return nil, fmt.Errorf("getting blocks: %w", err)
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

	bm, tm := make(blockmap), make(txmap)
	for i := range blocks {
		bm[blocks[i].Num()] = &blocks[i]
		for j := range blocks[i].Txs {
			t := &blocks[i].Txs[j]
			k := key{
				b: blocks[i].Num(),
				t: uint64(t.Idx),
			}
			tm[k] = t
		}
	}

	switch {
	case filter.UseReceipts:
		if err := c.receipts(bm, tm, blocks); err != nil {
			return nil, fmt.Errorf("getting receipts: %w", err)
		}
	case filter.UseLogs:
		if err := c.logs(filter, bm, tm, blocks); err != nil {
			return nil, fmt.Errorf("getting logs: %w", err)
		}
	}
	return blocks, nil
}

func (c *Client) LoadBlocks(filter *glf.Filter, blocks []eth.Block) error {
	switch {
	case filter.UseBlocks:
		if err := c.blocks(blocks); err != nil {
			return fmt.Errorf("getting blocks: %w", err)
		}
	case filter.UseHeaders:
		if err := c.headers(blocks); err != nil {
			return fmt.Errorf("getting headers: %w", err)
		}
	}

	bm, tm := make(blockmap), make(txmap)
	for i := range blocks {
		bm[blocks[i].Num()] = &blocks[i]
		for j := range blocks[i].Txs {
			t := &blocks[i].Txs[j]
			k := key{
				b: blocks[i].Num(),
				t: uint64(t.Idx),
			}
			tm[k] = t
		}
	}

	switch {
	case filter.UseReceipts:
		if err := c.receipts(bm, tm, blocks); err != nil {
			return fmt.Errorf("getting receipts: %w", err)
		}
	case filter.UseLogs:
		if err := c.logs(filter, bm, tm, blocks); err != nil {
			return fmt.Errorf("getting logs: %w", err)
		}
	}
	return nil
}

type blockResp struct {
	Error      `json:"error"`
	*eth.Block `json:"result"`
}

type blockCache []eth.Block

func (bc *blockCache) push(req []eth.Block) {
	*bc = append((*bc)[len(req):], req...)
}

func (bc *blockCache) get2(start, limit uint64) ([]eth.Block, bool) {
	var m, n int = -1, -1
	for i := range *bc {
		switch (*bc)[i].Num() {
		case start:
			m = i
		case start - 1 + limit:
			n = i + 1
			break
		}
	}
	if m < 0 || n < 0 {
		return nil, false
	}
	if n-m != int(limit) {
		panic(fmt.Sprintf("start=%d limit=%d m=%d n=%d\n", start, limit, m, n))
	}
	return (*bc)[m:n], true
}

func (c *Client) blocks2(start, limit uint64) ([]eth.Block, error) {
	c.cacheMut.Lock()
	defer c.cacheMut.Unlock()
	if b, ok := c.bcache.get2(start, limit); ok {
		return b, nil
	}
	fmt.Println("blocks2")
	var (
		reqs   = make([]request, limit)
		resps  = make([]blockResp, limit)
		blocks = make([]eth.Block, limit)
	)
	for i := uint64(0); i < limit; i++ {
		reqs[i] = request{
			ID:      "1",
			Version: "2.0",
			Method:  "eth_getBlockByNumber",
			Params:  []any{eth.EncodeUint64(start + i), true},
		}
		resps[i].Block = &blocks[i]
	}
	err := c.do(&resps, reqs)
	if err != nil {
		return nil, fmt.Errorf("requesting blocks: %w", err)
	}
	for i := range resps {
		if resps[i].Error.Exists() {
			const tag = "eth_getBlockByNumber"
			return nil, fmt.Errorf("rpc=%s %w", tag, resps[i].Error)
		}
	}
	c.bcache.push(blocks)
	return blocks, nil
}

type headerResp struct {
	Error       `json:"error"`
	*eth.Header `json:"result"`
}

func (c *Client) headers2(start, limit uint64) ([]eth.Block, error) {
	fmt.Printf("headers2 %d %d\n", start, limit)
	c.cacheMut.Lock()
	defer c.cacheMut.Unlock()
	if b, ok := c.hcache.get2(start, limit); ok {
		return b, nil
	}
	var (
		reqs   = make([]request, limit)
		resps  = make([]headerResp, limit)
		blocks = make([]eth.Block, limit)
	)
	for i := uint64(0); i < limit; i++ {
		reqs[i] = request{
			ID:      "1",
			Version: "2.0",
			Method:  "eth_getBlockByNumber",
			Params:  []any{eth.EncodeUint64(start + i), false},
		}
		resps[i].Header = &blocks[i].Header
	}
	err := c.do(&resps, reqs)
	if err != nil {
		return nil, fmt.Errorf("requesting headers: %w", err)
	}
	for i := range resps {
		if resps[i].Error.Exists() {
			const tag = "eth_getBlockByNumber/headers"
			return nil, fmt.Errorf("rpc=%s %w", tag, resps[i].Error)
		}
	}
	c.hcache.push(blocks)
	return blocks, nil
}

type receiptResult struct {
	BlockHash eth.Bytes  `json:"blockHash"`
	BlockNum  eth.Uint64 `json:"blockNumber"`
	TxHash    eth.Bytes  `json:"transactionHash"`
	TxIdx     eth.Uint64 `json:"transactionIndex"`
	TxType    eth.Byte   `json:"type"`
	TxFrom    eth.Bytes  `json:"from"`
	TxTo      eth.Bytes  `json:"to"`
	Status    eth.Byte   `json:"status"`
	GasUsed   eth.Uint64 `json:"gasUsed"`
	Logs      eth.Logs   `json:"logs"`
}

type receiptResp struct {
	Error  `json:"error"`
	Result []receiptResult `json:"result"`
}

func (c *Client) receipts(bm blockmap, tm txmap, blocks []eth.Block) error {
	var (
		reqs  = make([]request, len(blocks))
		resps = make([]receiptResp, len(blocks))
	)
	for i := range blocks {
		reqs[i] = request{
			ID:      "1",
			Version: "2.0",
			Method:  "eth_getBlockReceipts",
			Params:  []any{eth.EncodeUint64(blocks[i].Num())},
		}
	}
	err := c.do(&resps, reqs)
	if err != nil {
		return fmt.Errorf("requesting blocks: %w", err)
	}
	for i := range resps {
		if resps[i].Error.Exists() {
			const tag = "eth_getBlockReceipts"
			return fmt.Errorf("rpc=%s %w", tag, resps[i].Error)
		}
	}
	for i := range resps {
		b, ok := bm[uint64(resps[i].Result[0].BlockNum)]
		if !ok {
			return fmt.Errorf("block not found")
		}
		b.Header.Hash.Write(resps[i].Result[0].BlockHash)
		for j := range resps[i].Result {
			k := key{
				b: b.Num(),
				t: uint64(resps[i].Result[j].TxIdx),
			}
			if tx, ok := tm[k]; ok {
				tx.Status.Write(byte(resps[i].Result[j].Status))
				tx.GasUsed = resps[i].Result[j].GasUsed
				tx.Logs = make([]eth.Log, len(resps[i].Result[j].Logs))
				copy(tx.Logs, resps[i].Result[j].Logs)
				continue
			}

			tx := eth.Tx{}
			tx.Idx = resps[i].Result[j].TxIdx
			tx.PrecompHash.Write(resps[i].Result[j].TxHash)
			tx.Type.Write(byte(resps[i].Result[j].TxType))
			tx.From.Write(resps[i].Result[j].TxFrom)
			tx.To.Write(resps[i].Result[j].TxTo)
			tx.Status.Write(byte(resps[i].Result[j].Status))
			tx.GasUsed = resps[i].Result[j].GasUsed
			tx.Logs = make([]eth.Log, len(resps[i].Result[j].Logs))
			copy(tx.Logs, resps[i].Result[j].Logs)
			b.Txs = append(b.Txs, tx)
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

func (c *Client) logs(filter *glf.Filter, bm blockmap, tm txmap, blocks []eth.Block) error {
	var (
		lf = struct {
			From    string     `json:"fromBlock"`
			To      string     `json:"toBlock"`
			Address []string   `json:"address"`
			Topics  [][]string `json:"topics"`
		}{
			From:    eth.EncodeUint64(blocks[0].Num()),
			To:      eth.EncodeUint64(blocks[len(blocks)-1].Num()),
			Address: filter.Addresses(),
			Topics:  filter.Topics(),
		}
		lresp = logResp{}
	)
	err := c.do(&lresp, request{
		ID:      "1",
		Version: "2.0",
		Method:  "eth_getLogs",
		Params:  []any{lf},
	})
	if err != nil {
		return fmt.Errorf("making logs request: %w", err)
	}
	if lresp.Error.Exists() {
		return fmt.Errorf("rpc=%s %w", "eth_getLogs", lresp.Error)
	}
	var logsByTx = map[key][]logResult{}
	for i := range lresp.Result {
		k := key{
			b: uint64(lresp.Result[i].BlockNum),
			t: uint64(lresp.Result[i].TxIdx),
		}
		if logs, ok := logsByTx[k]; ok {
			logsByTx[k] = append(logs, lresp.Result[i])
			continue
		}
		logsByTx[k] = []logResult{lresp.Result[i]}
	}
	for k, logs := range logsByTx {
		b, ok := bm[k.b]
		if !ok {
			return fmt.Errorf("block not found")
		}
		if tx, ok := tm[k]; ok {
			for i := range logs {
				tx.Logs = append(tx.Logs, *logs[i].Log)
			}
			continue
		}
		tx := eth.Tx{}
		tx.Idx = eth.Uint64(k.t)
		tx.PrecompHash.Write(logs[0].TxHash)
		tx.Logs = make([]eth.Log, 0, len(logs))
		for i := range logs {
			tx.Logs.Add(logs[i].Log)
		}
		b.Header.Hash.Write(logs[0].BlockHash)
		b.Header.Number = eth.Uint64(logs[0].BlockNum)
		b.Txs = append(b.Txs, tx)
	}
	return nil
}
