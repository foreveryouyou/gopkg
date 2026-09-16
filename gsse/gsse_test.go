package gsse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// -------------------------------------------------------------------------------------
// 编码：分片写入（回归「data 含换行就破帧」）
// -------------------------------------------------------------------------------------

func TestMessageWriteTo(t *testing.T) {
	cases := []struct {
		name string
		data string
		want string
	}{
		{"plain", "hello", "data: hello\n\n"},
		{"lf", "a\nb", "data: a\ndata: b\n\n"},
		{"crlf", "a\r\nb", "data: a\ndata: b\n\n"},
		{"cr", "a\rb", "data: a\ndata: b\n\n"},
		// 关键：\n\n 不能把一条消息拆成两条（客户端解码回来仍是 "a\n\nb"）
		{"blank line", "a\n\nb", "data: a\ndata: \ndata: b\n\n"},
		{"empty", "", "data: \n\n"},
		{"trailing lf", "a\n", "data: a\ndata: \n\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := NewMessage(c.data).String(); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestMessageWriteTo_Fields(t *testing.T) {
	cases := []struct {
		name string
		msg  *Message
		want string
	}{
		{"comment", NewCommentMessage("a\nb"), ": a\n: b\n\n"},
		{"retry", NewMessage("x", WithRetry(1500*time.Millisecond)), "retry: 1500\ndata: x\n\n"},
		{"retry sub-ms", NewMessage("x", WithRetry(500*time.Microsecond)), "data: x\n\n"},
		{"id/event sanitized", NewMessage("x", WithID("1\n2"), WithType("ev\nt")), "id: 1 2\nevent: ev t\ndata: x\n\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.msg.String(); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

// -------------------------------------------------------------------------------------
// 服务端：排空退出（回归「Stop 丢掉队列里最后一条 done 事件」）
// -------------------------------------------------------------------------------------

func TestServe_DrainsQueueOnStop(t *testing.T) {
	const rounds, batch = 50, 5

	for i := range rounds {
		s := NewSSEServer()
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = s.Serve(w, r)
		}))

		var wg sync.WaitGroup
		wg.Go(func() {
			for range batch {
				_ = s.SendMessage("m")
			}
			// 用完时是「发 done 事件 + Stop」两步，done 必须被写出
			_ = s.SendMessage("{}", WithType("done"))
			s.Stop()
		})

		resp, err := http.Get(ts.URL)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		ts.Close()
		wg.Wait()

		got := string(body)
		if !strings.Contains(got, "event: done\n") {
			t.Fatalf("round %d: lost done event, body=%q", i, got)
		}
		if n := strings.Count(got, "data: m\n"); n != batch {
			t.Fatalf("round %d: got %d messages, want %d, body=%q", i, n, batch, got)
		}
	}
}

// -------------------------------------------------------------------------------------
// 服务端：写失败即断连（回归「写错误被吞、生产者被永久阻塞」）
// -------------------------------------------------------------------------------------

var errWriteFailed = errors.New("write failed")

type failWriter struct {
	header http.Header
}

func (f *failWriter) Header() http.Header {
	if f.header == nil {
		f.header = make(http.Header)
	}
	return f.header
}

func (f *failWriter) Write([]byte) (int, error) { return 0, errWriteFailed }
func (f *failWriter) WriteHeader(int)           {}

func TestServe_WriteErrorClosesServer(t *testing.T) {
	s := NewSSEServer()

	if err := s.Serve(&failWriter{}, nil); !errors.Is(err, errWriteFailed) {
		t.Fatalf("Serve: got %v, want %v", err, errWriteFailed)
	}
	// 连接已断：后续发送必须立即失败，而不是阻塞
	if err := s.SendMessage("x"); !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("SendMessage: got %v, want %v", err, ErrChannelClosed)
	}
	if cause := s.Cause(); !errors.Is(cause, CancelErrDone) {
		t.Fatalf("Cause: got %v, want %v", cause, CancelErrDone)
	}
	if err := s.Serve(httptest.NewRecorder(), nil); !errors.Is(err, ErrAlreadyServed) {
		t.Fatalf("second Serve: got %v, want %v", err, ErrAlreadyServed)
	}
}

// -------------------------------------------------------------------------------------
// 服务端：生命周期与并发
// -------------------------------------------------------------------------------------

func TestServe_OnlyOnce(t *testing.T) {
	s := NewSSEServer()
	go func() { _ = s.Serve(httptest.NewRecorder(), nil) }()
	waitServed(t, s)

	s.Stop()
	<-s.Done()

	if err := s.Serve(httptest.NewRecorder(), nil); !errors.Is(err, ErrAlreadyServed) {
		t.Fatalf("got %v, want %v", err, ErrAlreadyServed)
	}
	if cause := s.Cause(); !errors.Is(cause, CancelErrStop) {
		t.Fatalf("Cause: got %v, want %v", cause, CancelErrStop)
	}
}

// TestSendMessage_ConcurrentWithStop 在 -race 下验证发送与关闭不会 panic / 数据竞争。
func TestSendMessage_ConcurrentWithStop(t *testing.T) {
	for range 100 {
		s := NewSSEServer()
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 50 {
					_ = s.SendMessage("x")
				}
			})
		}
		go func() { <-s.Done() }()
		wg.Wait()
	}
}

func waitServed(t *testing.T, s *SSEServer) {
	t.Helper()
	for range 1000 {
		s.mu.RLock()
		served := s.served
		s.mu.RUnlock()
		if served {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Serve did not start in time")
}

// -------------------------------------------------------------------------------------
// 客户端：与服务端往返（回归「同一 event 的多行 data 互相覆盖」
//              「strings.TrimSpace 吃掉 payload 首尾空白」）
// -------------------------------------------------------------------------------------

// openWith 起一个真实的 SSE 服务端，发送 opts 指定的消息后 Stop，
// 返回客户端解析出的事件序列。
func openWith(t *testing.T, produce func(s *SSEServer)) []Event {
	t.Helper()

	s := NewSSEServer()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = s.Serve(w, r)
	}))
	defer ts.Close()

	var wg sync.WaitGroup
	wg.Go(func() {
		produce(s)
		s.Stop()
	})

	req, err := http.NewRequest(http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	c := NewSSEClient(nil, ts.Client())
	var got []Event
	c.OnMessage(func(e Event) { got = append(got, e) })

	if err := c.Open(context.Background(), req); err != nil {
		t.Fatalf("Open: %v", err)
	}
	wg.Wait()

	return got
}

func TestClient_MultiLineDataIsJoined(t *testing.T) {
	// "\n\n" 必须原样还原：服务端会把它编成 "data: a\ndata: \ndata: b"，
	// 旧实现只用最后一行覆盖 event.Data，客户端会拿到 "b"。
	msgs := []struct {
		data string
		opts []MsgOpt
	}{
		{"hello", nil},
		{"a\nb", nil},
		{"a\n\nb", nil},
		{"  padded  ", nil},
		{"x", []MsgOpt{WithID("42"), WithType("update")}},
	}

	got := openWith(t, func(s *SSEServer) {
		for _, m := range msgs {
			_ = s.SendMessage(m.data, m.opts...)
		}
	})

	if len(got) != len(msgs) {
		t.Fatalf("got %d events, want %d: %+v", len(got), len(msgs), got)
	}
	for i, m := range msgs {
		if got[i].Data != m.data {
			t.Fatalf("event %d: Data=%q, want %q", i, got[i].Data, m.data)
		}
	}

	last := got[len(got)-1]
	if last.ID != "42" || last.Type != "update" {
		t.Fatalf("last event: got ID=%q Type=%q, want 42/update", last.ID, last.Type)
	}
	// LastEventID 是派发该事件时的 last event ID buffer，没有 id: 行的会沿用上一个值
	if last.LastEventID != "42" {
		t.Fatalf("last event: LastEventID=%q, want 42", last.LastEventID)
	}
}

// TestClient_IgnoresCommentAndEmptyData 注释行（含服务端建连时的 ": connected" 与心跳）
// 和空 data 都不应触发回调。
func TestClient_IgnoresCommentAndEmptyData(t *testing.T) {
	got := openWith(t, func(s *SSEServer) {
		_ = s.SendCommentMessage("ping")
		_ = s.SendMessage("")
		_ = s.SendMessage("real")
		_ = s.SendCommentMessage("ping")
	})

	if len(got) != 1 {
		t.Fatalf("got %d events, want 1: %+v", len(got), got)
	}
	if got[0].Data != "real" {
		t.Fatalf("Data=%q, want %q", got[0].Data, "real")
	}
}

// openRaw 直接喂一段 SSE 文本给客户端，用于构造服务端编码层表达不出来的帧（如空 id:）。
func openRaw(t *testing.T, frames ...string) []Event {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, frame := range frames {
			_, _ = io.WriteString(w, frame)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := NewSSEClient(nil, srv.Client())
	var got []Event
	c.OnMessage(func(e Event) { got = append(got, e) })

	if err := c.Open(context.Background(), req); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return got
}

// TestClient_LastEventIDFollowsSpec 空 id: 行按规范把 last event ID 重置为空，
// 而没有 id: 行的事件块沿用上一个值。
func TestClient_LastEventIDFollowsSpec(t *testing.T) {
	got := openRaw(t,
		"id: 1\ndata: a\n\n",
		"data: b\n\n",      // 没有 id: 行 —— 沿用 "1"
		"id:\ndata: c\n\n", // 空 id: 行 —— 重置为空
		"data: d\n\n",      // 沿用 ""
	)

	want := []struct{ data, lastID string }{
		{"a", "1"},
		{"b", "1"},
		{"c", ""},
		{"d", ""},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].Data != w.data || got[i].LastEventID != w.lastID {
			t.Fatalf("event %d: got Data=%q LastEventID=%q, want %q/%q",
				i, got[i].Data, got[i].LastEventID, w.data, w.lastID)
		}
	}
}

// TestClient_CancelInterruptsBlockedRead 服务端发完一条后不再出声时，
// ctx 取消必须能让阻塞在 Read 上的 Open 立刻返回。
func TestClient_CancelInterruptsBlockedRead(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: one\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release // 之后不再发任何数据，客户端会一直阻塞在读上
	}))
	defer func() {
		close(release)
		srv.Close()
	}()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	gotOne := make(chan struct{})
	c := NewSSEClient(func(Event) { close(gotOne) }, srv.Client())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Open(ctx, req) }()

	select {
	case <-gotOne:
	case <-time.After(5 * time.Second):
		t.Fatal("first event not received")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Open: got %v, want %v", err, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Open did not return after ctx cancel")
	}
}

// TestClient_ReconnectResumesWithLastEventID 同一个 client 重复 Open（重连）时，
// 自动带上上一次收到的 id；调用方显式设置过就不覆盖，且不污染调用方的 req。
func TestClient_ReconnectResumesWithLastEventID(t *testing.T) {
	ids := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids <- r.Header.Get("Last-Event-ID")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "id: 7\ndata: a\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer srv.Close()

	c := NewSSEClient(nil, srv.Client())

	open := func(setHeader string) *http.Request {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if setHeader != "" {
			req.Header.Set("Last-Event-ID", setHeader)
		}
		if err := c.Open(context.Background(), req); err != nil {
			t.Fatalf("Open: %v", err)
		}
		return req
	}

	// 首次连接：还没有 id 可续传
	open("")
	if got := <-ids; got != "" {
		t.Fatalf("first Open: Last-Event-ID=%q, want empty", got)
	}
	// 重连：自动续传上一轮的 "7"
	open("")
	if got := <-ids; got != "7" {
		t.Fatalf("reconnect: Last-Event-ID=%q, want 7", got)
	}
	// 调用方显式设置：不覆盖
	open("mine")
	if got := <-ids; got != "mine" {
		t.Fatalf("explicit header: Last-Event-ID=%q, want mine", got)
	}
	// 自动补的头只写在内部副本上，调用方的 req 保持原样
	if req := open(""); req.Header.Get("Last-Event-ID") != "" {
		t.Fatalf("Open mutated the caller's request: %q", req.Header.Get("Last-Event-ID"))
	}
	if got := <-ids; got != "7" {
		t.Fatalf("after explicit header: Last-Event-ID=%q, want 7", got)
	}
	if got := c.LastEventID(); got != "7" {
		t.Fatalf("LastEventID(): got %q, want 7", got)
	}
}
