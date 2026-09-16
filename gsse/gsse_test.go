package gsse

import (
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
