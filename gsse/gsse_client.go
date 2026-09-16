package gsse

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// SSEClient 是 SSE 客户端，一次 Open 对应一条连接。
//
// 同一个客户端可以反复 Open 来做重连：lastEventID 会跨 Open 保留，重连时会自动
// 补上 Last-Event-ID 头。但客户端必须**串行使用**——msgHandlers 与 lastEventID
// 都没有加锁，请勿并发调用 Open / OnMessage。
type SSEClient struct {
	msgHandlers []func(event Event)

	hc *http.Client

	// lastEventID 是 SSE 的 last event ID buffer：出现 id: 行时被覆盖（空值即重置为空），
	// 没有 id: 行的事件块沿用上一个值。它既填充 Event.LastEventID，也是重连续传的依据。
	lastEventID string
}

// NewSSEClient 创建客户端，请求由每次 Open 传入。
//
// onMessage 为 nil 表示暂不注册回调，之后可以用 OnMessage 追加。
// hc 可选：省略或传 nil 都用 http.DefaultClient。
//
// 别给 hc 设 Timeout：它是「整个请求含读 body」的上限，会把长连接掐断；
// 需要超时请用 context，或 Transport 层的 ResponseHeaderTimeout / IdleConnTimeout。
func NewSSEClient(onMessage func(event Event), hc ...*http.Client) *SSEClient {
	c := &SSEClient{hc: http.DefaultClient}
	if len(hc) > 0 && hc[0] != nil {
		c.hc = hc[0]
	}
	c.OnMessage(onMessage)
	return c
}

// The Event struct represents an event sent to the client by a server.
type Event struct {
	// The SSE "last event ID buffer" at the time the event was dispatched: it
	// changes only when the stream carries an "id:" field (an empty "id:" resets
	// it), so an event without an "id:" field carries the previous value over.
	LastEventID string

	// The event's ID.
	ID string
	// The event's type. It is empty if the event is unnamed.
	Type string
	// The event's payload.
	Data string
}

// OnMessage 追加一个消息回调，按注册顺序调用。请在 Open 之前注册。
func (c *SSEClient) OnMessage(hdl func(event Event)) {
	if hdl == nil {
		return
	}
	c.msgHandlers = append(c.msgHandlers, hdl)
}

// LastEventID 返回已收到事件里最后出现的 id（空 id: 行会把它重置为空）。
// 想自己掌控重连逻辑时可以拿它来设置 Last-Event-ID。
func (c *SSEClient) LastEventID() string {
	return c.lastEventID
}

// Open 用调用方传入的请求建一次连，阻塞到这条连接结束，返回结束原因。
//
// req 由调用方构造，所以 header / URL / 认证等完全可控；Open 不会修改传入的 req
// （内部先 Clone 出一份再绑定 ctx）。ctx 取消会立即关闭连接并返回 ctx.Err()，
// 因而可以用 errors.Is(err, context.Canceled) 判断「该停了」而不是「该重试」。
//
// 重连请在每次 Open 时新建一个请求：*http.Request 不是可复用对象。若之前收到过
// id，Open 会自动为这次请求补上 Last-Event-ID 头；调用方自己设置过则不覆盖。
func (c *SSEClient) Open(ctx context.Context, req *http.Request) (err error) {
	if c.hc == nil || req == nil {
		return errors.New("error: http.Client or http.Request is nil")
	}

	// Clone 出独立副本：绑定本次 Open 的 ctx，同时避免后面写 header 影响到调用方的 req。
	req = req.Clone(ctx)
	if c.lastEventID != "" && req.Header.Get("Last-Event-ID") == "" {
		req.Header.Set("Last-Event-ID", c.lastEventID)
	}

	// 发起请求
	resp, err := c.hc.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		// 读取错误响应的body内容
		var body string
		if bodyBytes, readErr := io.ReadAll(resp.Body); readErr == nil {
			body = string(bodyBytes)
		}
		err = fmt.Errorf("[StatusCode: %d] %s", resp.StatusCode, body)
		return
	}

	// scanner.Scan 阻塞在底层读上时不会去看 ctx，所以单独起一个 goroutine：
	// ctx 一旦取消就关掉 body，让 Scan 立刻返回，Open 得以按 ctx.Err() 退出。
	// bodyDone 保证这个 goroutine 在正常情况下也会退出，不会泄漏。
	bodyDone := make(chan struct{})
	defer close(bodyDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = resp.Body.Close()
		case <-bodyDone:
		}
	}()

	// 使用Scanner读取响应体
	const maxCapacity = 10 * 1024 * 1024 // 10MB
	buffer := make([]byte, maxCapacity)
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(buffer, maxCapacity)

	var (
		event Event
		// data 收集当前事件块的 data 行。同一事件块的多个 data: 行按规范用 \n 拼接，
		// 而不是互相覆盖，否则多行 payload 只会剩下最后一行。
		data strings.Builder
		// dataLines 记录已收到的 data: 行数（含空行），用于决定拼接时是否补 \n。
		dataLines int
	)

	// dispatch 派发当前事件块并重置缓冲区。
	dispatch := func() {
		// 规范：一个事件块里没有 data 时不派发事件（只有注释行、只有 id/event 行都算），
		// 这样服务端的心跳注释 ": ping" 不会被当成一个个空事件。
		if data.Len() > 0 {
			event.Data = data.String()
			event.LastEventID = c.lastEventID
			c.handleEvent(event)
		}
		event = Event{}
		data.Reset()
		dataLines = 0
	}

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()
		if line == "" {
			// 当前为空行：一个事件块结束
			dispatch()
			continue
		}

		// 在这里添加逻辑来解析事件类型和数据
		// 例如，处理特定事件类型或结束信号等
		if strings.HasPrefix(line, ":") {
			// 这是一个注释，忽略它
			continue
		}

		if after, ok := strings.CutPrefix(line, "event:"); ok {
			// 这是一个事件类型
			event.Type = trimOneLeadingSpace(after)
			continue
		}
		if after, ok := strings.CutPrefix(line, "id:"); ok {
			// 这是一个ID行。规范：id 的值直接覆盖 last event ID buffer，
			// 空值即把它重置为空，所以这里不再要求「非空」。
			event.ID = trimOneLeadingSpace(after)
			c.lastEventID = event.ID
			continue
		}
		if after, ok := strings.CutPrefix(line, "data:"); ok {
			// 这是一个数据行
			if dataLines > 0 {
				data.WriteByte('\n')
			}
			dataLines++
			data.WriteString(trimOneLeadingSpace(after))
		}
	}
	// ctx 被取消时不再派发半截数据，直接以取消原因返回。
	if err := ctx.Err(); err != nil {
		return err
	}
	// 防止结尾没有空行时漏掉最后一个event
	dispatch()

	if err := scanner.Err(); err != nil {
		return err
	}

	return
}

// handleEvent 处理事件回调
func (c *SSEClient) handleEvent(event Event) {
	// 处理接收到的事件数据
	// fmt.Println("Received:", event)
	for _, hdl := range c.msgHandlers {
		hdl(event)
	}
}

// trimOneLeadingSpace 按 SSE 规范只去掉字段值开头的一个空格：
//
//	"data: foo"  -> "foo"
//	"data:  foo" -> " foo"
//	"data: "     -> ""
//
// 不能用 strings.TrimSpace：它会一并吃掉 payload 的首尾空白，而空白是数据的一部分。
func trimOneLeadingSpace(s string) string {
	return strings.TrimPrefix(s, " ")
}
