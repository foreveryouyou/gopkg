// Package gsse 提供只依赖标准库 net/http 的 SSE（Server-Sent Events）实现：
// 服务端 SSEServer 把连接升级成事件流，客户端 SSEClient 消费事件流。
//
// 服务端：一个连接对应一个 SSEServer，生产者调用 SendMessage 入队，Serve 在唯一一个 goroutine 中
// 消费队列并写回连接；Stop/Done 会关闭队列，并等待已入队的消息写完后再退出。
package gsse

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

var (
	ErrChannelClosed = errors.New("sse server channel is closed")
	ErrSendTimeout   = errors.New("sse server send timeout")
	ErrNilWriter     = errors.New("sse server response writer is nil")
	ErrAlreadyServed = errors.New("sse server has already been served")

	CancelErrDone = errors.New("sse server done")
	CancelErrStop = errors.New("sse server stop")
)

const (
	// DefaultBufferSize 消息队列默认容量。
	DefaultBufferSize = 100
	// DefaultHeartbeatInterval 默认心跳间隔。
	DefaultHeartbeatInterval = 10 * time.Second
	// DefaultHeartbeatMessage 默认心跳内容（以注释形式发送）。
	DefaultHeartbeatMessage = "ping"
	// DefaultSendTimeout 队列写满时 SendMessage 的最长等待时间，避免生产者被永久阻塞。
	DefaultSendTimeout = 5 * time.Second
)

// -------------------------------------------------------------------------------------

// SSEServer 封装单个 SSE 连接的服务端。
//
// 一个实例只能 Serve 一次；Serve 返回后该实例即失效。
type SSEServer struct {
	out    chan *Message
	ctx    context.Context
	cancel context.CancelCauseFunc

	heartbeatInterval time.Duration
	heartbeatMsg      string
	sendTimeout       time.Duration

	serveDone     chan struct{}
	serveDoneOnce sync.Once

	// mu 保护 closed / served。
	// 发送与关闭都在这把锁下完成，保证“向已关闭 channel 发送”不会发生 panic。
	mu     sync.RWMutex
	closed bool
	served bool
}

// NewSSEServer 创建服务端实例：一个连接一个实例，且只能 Serve 一次。
func NewSSEServer() *SSEServer {
	s := &SSEServer{
		out:               make(chan *Message, DefaultBufferSize),
		heartbeatInterval: DefaultHeartbeatInterval,
		heartbeatMsg:      DefaultHeartbeatMessage,
		sendTimeout:       DefaultSendTimeout,
		serveDone:         make(chan struct{}),
	}
	// ctx/cancel 在构造时就创建好，之后只读，避免 Serve 与 Stop/SendMessage 之间的 data race。
	s.ctx, s.cancel = context.WithCancelCause(context.Background())
	return s
}

// Serve 把当前连接升级为 SSE 流，阻塞直到客户端断开、Stop/Done 被调用或写回失败。
//
// 返回 nil 表示“正常结束”（队列已排空）；返回其它错误表示连接异常或复用实例。
func (s *SSEServer) Serve(w http.ResponseWriter, r *http.Request) error {
	if w == nil {
		return ErrNilWriter
	}
	if !s.markServed() {
		return ErrAlreadyServed
	}
	defer func() {
		s.close(CancelErrDone)
		s.markServeDone()
	}()

	rc := http.NewResponseController(w)

	// 立刻下发响应头 + flush：否则客户端可能要等到第一条业务消息（最长一个心跳周期）才触发 onopen。
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	// 关掉 nginx 等反向代理的响应缓冲，保证逐条下发。
	// 注意：不要设置 Connection: keep-alive，HTTP/2 下无意义且属于被禁止的 hop-by-hop 头。
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := flush(rc); err != nil {
		return err
	}

	// 先发一条注释，让 EventSource / fetch 立刻确认连接已建立。
	if err := s.writeMessage(w, rc, NewCommentMessage("connected")); err != nil {
		return err
	}

	s.startHeartbeat()

	var clientGone <-chan struct{}
	if r != nil {
		clientGone = r.Context().Done()
	}

	for {
		select {
		case <-clientGone:
			return r.Context().Err()
		case msg, ok := <-s.out:
			if !ok {
				// 队列已关闭：Go 会先把缓冲区里的消息读完才会返回 !ok，
				// 所以这里返回意味着“已入队的消息全部写完”，done 事件不会丢。
				return nil
			}
			if err := s.writeMessage(w, rc, msg); err != nil {
				return err
			}
		}
	}
}

// ServeHTTP 让 SSEServer 可以直接当作 http.Handler 使用。
func (s *SSEServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = s.Serve(w, r)
}

// Stop 结束 SSE 服务：关闭队列、停止心跳，并唤醒所有阻塞中的发送者。
//
// Stop 只负责发出结束信号；队列中已经入队的消息会被 Serve 排空后写出。
func (s *SSEServer) Stop() {
	s.close(CancelErrStop)
}

// Done 优雅结束本次 SSE 流，并返回一个在 Serve 退出（队列中已入队的消息全部写出）后关闭的 channel。
//
//	<-sse.Done() // 需要等待收尾完成时
//	sse.Done()   // 只触发结束、不等待时
func (s *SSEServer) Done() <-chan struct{} {
	s.Stop()
	return s.serveDone
}

// Cause 返回服务结束的原因（CancelErrDone / CancelErrStop）。
func (s *SSEServer) Cause() error {
	return context.Cause(s.ctx)
}

// -------------------------------------------------------------------------------------

// SendMessage 把消息入队，由 Serve 所在的 goroutine 写回连接。
func (s *SSEServer) SendMessage(msg string, opts ...MsgOpt) error {
	return s.sendMessage(NewMessage(msg, opts...))
}

// SendCommentMessage 发送一条注释消息（通常用于心跳）。
func (s *SSEServer) SendCommentMessage(data string) error {
	return s.sendMessage(NewCommentMessage(data))
}

// -------------------------------------------------------------------------------------

func (s *SSEServer) sendMessage(msg *Message) error {
	if msg == nil {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrChannelClosed
	}

	// 队列未满时直接入队，避免每条消息都创建定时器。
	select {
	case s.out <- msg:
		return nil
	default:
	}

	// 队列已满：带超时与取消地等待，既不永久阻塞生产者，也不静默丢消息。
	timer := time.NewTimer(s.sendTimeout)
	defer timer.Stop()

	select {
	case s.out <- msg:
		return nil
	case <-s.ctx.Done():
		return ErrChannelClosed
	case <-timer.C:
		return ErrSendTimeout
	}
}

// writeMessage 把 msg 写回连接，并把底层的写/刷错误透传给调用方；出错即表示连接已断开。
func (s *SSEServer) writeMessage(w http.ResponseWriter, rc *http.ResponseController, msg *Message) error {
	if w == nil || msg == nil {
		return nil
	}
	if _, err := msg.WriteTo(w); err != nil {
		return err
	}
	return flush(rc)
}

// flush 触发一次刷新；不支持 Flush 的 writer 不视为错误。
func flush(rc *http.ResponseController) error {
	if err := rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// startHeartbeat 定时发送注释消息，防止中间代理因空闲而断开连接。
func (s *SSEServer) startHeartbeat() {
	go func() {
		ticker := time.NewTicker(s.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// 仅在服务已结束时退出；队列瞬时写满（ErrSendTimeout）不应终止心跳。
				if err := s.SendCommentMessage(s.heartbeatMsg); err != nil && errors.Is(err, ErrChannelClosed) {
					return
				}
			case <-s.ctx.Done():
				return
			}
		}
	}()
}

// close 关闭队列并取消 ctx，可重复调用。
func (s *SSEServer) close(cause error) {
	// 先取消 ctx：尽快唤醒阻塞在发送上的生产者与心跳 goroutine，
	// 也让随后获取写锁的 close 不会被长时间阻塞。
	s.cancel(cause)

	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.out)
	}
	served := s.served
	s.mu.Unlock()

	// Serve 尚未启动时没人会去关 serveDone，这里直接通知等待者，避免 Done() 永久阻塞。
	if !served {
		s.markServeDone()
	}
}

// markServed 标记 Serve 已启动；重复启动返回 false。
func (s *SSEServer) markServed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.served {
		return false
	}
	s.served = true
	return true
}

func (s *SSEServer) markServeDone() {
	s.serveDoneOnce.Do(func() {
		close(s.serveDone)
	})
}
