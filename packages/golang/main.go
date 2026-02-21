package xeondbdriver

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type Options struct {
	Host     string
	Port     int
	Username string
	Password string
}

type XeondbClient struct {
	host     string
	port     int
	username string
	password string

	conn      net.Conn
	connected bool
	keyspace  string

	reqMu   sync.Mutex
	pendMu  sync.Mutex
	pending []*pendingReq

	closeOnce sync.Once
}

type pendingReq struct {
	ch chan pendingResp
}

type pendingResp struct {
	line string
	err  error
}

func NewXeondbClient(opts Options) *XeondbClient {
	host := opts.Host
	if strings.TrimSpace(host) == "" {
		host = "127.0.0.1"
	}
	port := opts.Port
	if port == 0 {
		port = 9876
	}
	return &XeondbClient{
		host:     host,
		port:     port,
		username: opts.Username,
		password: opts.Password,
		pending:  make([]*pendingReq, 0),
	}
}

func (c *XeondbClient) Connect() bool {
	addr := fmt.Sprintf("%s:%d", c.host, c.port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return false
	}

	c.conn = conn
	c.connected = true
	c.installReader()

	username := strings.TrimSpace(c.username) != ""
	password := strings.TrimSpace(c.password) != ""
	if username || password {
		res, err := c.Auth(c.username, c.password)
		if err != nil {
			c.Close()
			return false
		}
		if ok, _ := res["ok"].(bool); !ok {
			c.Close()
			return false
		}
	}

	return true
}

func (c *XeondbClient) Auth(username, password string) (map[string]any, error) {
	u := sqlQuoted(username)
	p := sqlQuoted(password)
	return c.Query("AUTH " + u + " " + p + ";")
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func isIdentifier(s string) bool {
	return identRe.MatchString(s)
}

func (c *XeondbClient) SelectKeyspace(keyspace string) error {
	if !c.connected {
		return errors.New("Not connected")
	}
	if !isIdentifier(keyspace) {
		return errors.New("Invalid keyspace")
	}
	res, err := c.Query("USE " + keyspace + ";")
	if err != nil {
		return err
	}
	if ok, _ := res["ok"].(bool); !ok {
		if e, _ := res["error"].(string); strings.TrimSpace(e) != "" {
			return errors.New(e)
		}
		return errors.New("Failed to select keyspace")
	}
	c.keyspace = keyspace
	return nil
}

func (c *XeondbClient) QueryRaw(cmd string) (string, error) {
	c.reqMu.Lock()

	if !c.connected || c.conn == nil {
		c.reqMu.Unlock()
		return "", errors.New("Not connected")
	}

	sql := strings.TrimSpace(fmt.Sprint(cmd))
	if sql == "" {
		c.reqMu.Unlock()
		return "", errors.New("Empty query")
	}

	p := &pendingReq{ch: make(chan pendingResp, 1)}
	c.pendMu.Lock()
	c.pending = append(c.pending, p)
	c.pendMu.Unlock()

	_, err := c.conn.Write([]byte(sql + "\n"))
	c.reqMu.Unlock()
	if err != nil {
		c.pendMu.Lock()
		if n := len(c.pending); n > 0 && c.pending[n-1] == p {
			c.pending = c.pending[:n-1]
		} else {
			for i := range c.pending {
				if c.pending[i] == p {
					c.pending = append(c.pending[:i], c.pending[i+1:]...)
					break
				}
			}
		}
		c.pendMu.Unlock()
		return "", err
	}

	resp := <-p.ch
	return resp.line, resp.err
}

func (c *XeondbClient) Query(cmd string) (map[string]any, error) {
	raw, err := c.QueryRaw(cmd)
	if err != nil {
		return nil, err
	}

	var out map[string]any
	if e := json.Unmarshal([]byte(raw), &out); e != nil {
		return map[string]any{"ok": false, "error": "Bad JSON", "raw": raw}, nil
	}
	return out, nil
}

func cellString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64, bool, int, int64, uint64:
		return fmt.Sprint(t)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(b)
	}
}

func truncateCell(s string, maxLen int) string {
	if maxLen <= 0 {
		maxLen = 60
	}
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func printTable(headers []string, rows [][]string) {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, r := range rows {
		for i := 0; i < len(headers) && i < len(r); i++ {
			if len(r[i]) > widths[i] {
				widths[i] = len(r[i])
			}
		}
	}

	parts := make([]string, len(widths))
	for i, w := range widths {
		parts[i] = strings.Repeat("-", w+2)
	}
	line := "+" + strings.Join(parts, "+") + "+"

	fmtRow := func(cols []string) string {
		out := make([]string, len(headers))
		for i := range headers {
			cell := ""
			if i < len(cols) {
				cell = cols[i]
			}
			out[i] = " " + cell + strings.Repeat(" ", widths[i]-len(cell)) + " "
		}
		return "|" + strings.Join(out, "|") + "|"
	}

	fmt.Println(line)
	fmt.Println(fmtRow(headers))
	fmt.Println(line)
	for _, r := range rows {
		fmt.Println(fmtRow(r))
	}
	fmt.Println(line)
}

func (c *XeondbClient) QueryTable(cmd string) (map[string]any, error) {
	res, err := c.Query(cmd)
	if err != nil {
		return nil, err
	}

	if v, ok := res["rows"]; ok {
		if rowsAny, ok := v.([]any); ok {
			if len(rowsAny) == 0 {
				fmt.Println("(no rows)")
				return res, nil
			}
			first, _ := rowsAny[0].(map[string]any)
			headers := make([]string, 0, len(first))
			for k := range first {
				headers = append(headers, k)
			}
			sort.Strings(headers)

			rows := make([][]string, 0, len(rowsAny))
			for _, r := range rowsAny {
				m, _ := r.(map[string]any)
				row := make([]string, len(headers))
				for i, h := range headers {
					row[i] = truncateCell(cellString(m[h]), 60)
				}
				rows = append(rows, row)
			}
			printTable(headers, rows)
			return res, nil
		}
	}

	if _, ok := res["found"]; ok {
		found, _ := res["found"].(bool)
		if !found {
			fmt.Println("(no rows)")
			return res, nil
		}
		rowObj, _ := res["row"].(map[string]any)
		headers := make([]string, 0, len(rowObj))
		for k := range rowObj {
			headers = append(headers, k)
		}
		sort.Strings(headers)
		values := make([]string, len(headers))
		for i, h := range headers {
			values[i] = truncateCell(cellString(rowObj[h]), 60)
		}
		printTable(headers, [][]string{values})
		return res, nil
	}

	keys := make([]string, 0, len(res))
	for k := range res {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rows := make([][]string, 0, len(keys))
	for _, k := range keys {
		rows = append(rows, []string{k, truncateCell(cellString(res[k]), 60)})
	}
	printTable([]string{"key", "value"}, rows)
	return res, nil
}

func (c *XeondbClient) ExecTable(cmd string) (bool, error) {
	res, err := c.QueryTable(cmd)
	if err != nil {
		return false, err
	}
	if res == nil {
		return false, nil
	}
	ok, _ := res["ok"].(bool)
	return ok, nil
}

func (c *XeondbClient) Close() {
	c.closeOnce.Do(func() {
		c.connected = false
		c.rejectAllPending(errors.New("Connection closed"))
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

func (c *XeondbClient) installReader() {
	if c.conn == nil {
		return
	}
	r := bufio.NewReader(c.conn)
	go func() {
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				c.connected = false
				if errors.Is(err, io.EOF) {
					c.rejectAllPending(errors.New("Connection closed"))
				} else {
					c.rejectAllPending(err)
				}
				return
			}
			line = strings.TrimSpace(line)
			p := c.shiftPending()
			if p == nil {
				continue
			}
			p.ch <- pendingResp{line: line, err: nil}
		}
	}()
}

func (c *XeondbClient) shiftPending() *pendingReq {
	c.pendMu.Lock()
	defer c.pendMu.Unlock()
	if len(c.pending) == 0 {
		return nil
	}
	p := c.pending[0]
	c.pending = c.pending[1:]
	return p
}

func (c *XeondbClient) rejectAllPending(err error) {
	c.pendMu.Lock()
	pending := c.pending
	c.pending = nil
	c.pendMu.Unlock()
	for _, p := range pending {
		if p == nil || p.ch == nil {
			continue
		}
		select {
		case p.ch <- pendingResp{line: "", err: err}:
		default:
		}
	}
}

func sqlQuoted(v string) string {
	s := fmt.Sprint(v)
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	s = strings.ReplaceAll(s, "\t", "\\t")
	return "\"" + s + "\""
}
