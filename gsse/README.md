# gsse

只依赖标准库 `net/http` 的 SSE（Server-Sent Events）实现，服务端与客户端各一个类型：

- `SSEServer`：把一个 HTTP 连接升级成 SSE 流，把生产者的消息推给客户端；
- `SSEClient`：消费 SSE 流，按规范解析事件块，支持断线重连续传。

两者互不依赖，只共享线路格式相关的定义（`Message` / `Event`）。

## SSEServer

### 设计

**一个连接对应一个 `SSEServer` 实例**：

- 生产者调用 `SendMessage` 把消息入队；
- `Serve` 在唯一一个 goroutine 中消费队列并写回连接；
- `Stop` / `Done` 关闭队列，`Serve` 会把已入队的消息**排空写完**再退出，不丢最后一条事件。

### 快速开始

```go
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/foreveryouyou/gopkg/gsse"
)

func main() {
	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		sse := gsse.NewSSEServer()

		// 生产者：持续推送
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for range ticker.C {
				err := sse.SendMessage("hello",
					gsse.WithID("1"),
					gsse.WithType("message"),
				)
				if err != nil { // 连接已结束，停止生产
					return
				}
			}
		}()

		// Serve 阻塞，直到客户端断开、Stop/Done 被调用或写回失败
		if err := sse.Serve(w, r); err != nil {
			log.Println("sse serve:", err)
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

`SSEServer` 也实现了 `http.Handler`，可直接作为 handler 注册：

```go
sse := gsse.NewSSEServer()
http.Handle("/events", sse) // 等价于 Serve(w, r)
```

### 主动结束

```go
sse.Stop()      // 只发结束信号，不等待
<-sse.Done()    // 结束并等待收尾完成（已入队消息全部写出后才返回）
```

`Cause()` 返回结束原因：`CancelErrStop`（Stop/Done）或 `CancelErrDone`（Serve 自身退出）。

### API

| 方法 | 说明 |
| --- | --- |
| `NewSSEServer() *SSEServer` | 创建实例，一个连接一个 |
| `Serve(w, r) error` | 升级为 SSE 流并阻塞；返回 `nil` 表示队列已排空、正常结束 |
| `ServeHTTP(w, r)` | 实现 `http.Handler` |
| `SendMessage(msg string, opts ...MsgOpt) error` | 入队一条消息 |
| `SendCommentMessage(data string) error` | 入队一条注释消息（通常用于心跳） |
| `Stop()` | 结束服务 |
| `Done() <-chan struct{}` | 结束服务并返回 Serve 退出后关闭的 channel |
| `Cause() error` | 返回结束原因 |

### 消息选项

```go
gsse.NewMessage("data",
	gsse.WithID("42"),                     // id 字段
	gsse.WithType("update"),               // event 字段
	gsse.WithRetry(1500*time.Millisecond), // retry 字段
)

gsse.NewCommentMessage("ping")             // 注释行（": ping"）
```

`Message.String()` 返回 SSE 线路格式，可直接落库 / 回放：

```go
msg := gsse.NewMessage("a\n\nb")
msg.String() // "data: a\ndata: \ndata: b\n\n"
```

`data` / 注释值会先把 `\r\n`、`\r`、`\n` 规范化为多行再逐行输出，值里含换行既不会截断内容，也不会多出一条消息；`id` / `event` 是单行字段，其中的换行会被替换为空格。

### 错误

| 错误 | 含义 |
| --- | --- |
| `ErrAlreadyServed` | 同一实例被 `Serve` 了第二次（实例只能 Serve 一次） |
| `ErrChannelClosed` | 服务已结束，`SendMessage` 立即失败，而不是阻塞 |
| `ErrSendTimeout` | 队列写满且超过等待上限（默认 5s），消息未入队 |
| `ErrNilWriter` | `Serve` 收到 nil 的 `http.ResponseWriter` |

### 内置行为

- **立即建连**：`Serve` 一开始就下发响应头和一条 `: connected` 注释并 flush，客户端无需等到首条业务消息。
- **心跳**：每 10s 发送一条注释消息（默认 `ping`），防止中间代理因空闲断连。
- **反代友好**：自动设置 `X-Accel-Buffering: no`，关闭 nginx 等代理的响应缓冲。
- **生产端不会被永久阻塞**：队列默认容量 100，写满时最长等 5s 后返回 `ErrSendTimeout`。

## SSEClient

### 快速开始

`Open` 接受调用方构造的请求（header 完全可控），一次调用对应一条连接，重连自己循环：

```go
c := gsse.NewSSEClient(func(e gsse.Event) {
	log.Println(e.Type, e.Data)
}) // 第二个参数可选：自定义 http.Client 时传 gsse.NewSSEClient(fn, hc)

for {
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:8080/events", nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer xxx")

	err = c.Open(context.Background(), req)
	if errors.Is(err, context.Canceled) { // 该停了，而不是该重试
		return
	}
	log.Println("sse lost, retrying:", err)
	time.Sleep(time.Second) // 上一轮的 id 会被自动补进 Last-Event-ID
}
```

### API

| 方法 | 说明 |
| --- | --- |
| `NewSSEClient(onMessage, hc...) *SSEClient` | 创建客户端，请求由每次 `Open` 传入；`hc` 可省略（或传 nil），缺省用 `http.DefaultClient` |
| `Open(ctx, req) error` | 建连并阻塞；`ctx` 取消立即关闭连接并返回 `ctx.Err()` |
| `OnMessage(hdl)` | 追加消息回调（需在 `Open` 前注册） |
| `LastEventID() string` | 当前 last event ID，可用于自定义重连 |

```go
type Event struct {
	LastEventID string // 派发该事件时的 last event ID buffer
	ID          string
	Type        string // 无名事件为空
	Data        string
}
```

### 使用约定

- **必须串行使用**：`OnMessage` / `Open` 不加锁，别并发调用同一个 client；需要并发就多建几个实例。
- **不要给 `hc` 设 `Timeout`**：它算的是「整个请求含读 body」，会把长连接掐断；要超时用 context 或 `Transport` 层的 `ResponseHeaderTimeout` / `IdleConnTimeout`。
- **`Open` 不修改传入的请求**：内部先 `Clone` 一份再绑定 ctx 和 `Last-Event-ID`，调用方的 `*http.Request` 保持原样。
- **重连要新建请求**：`*http.Request` 不是可复用对象，每次 `Open` 都重新构造一个。

### 重连与 Last-Event-ID

`Open` 跨调用保留 last event ID，所以重连是免费续传的：

- 之前收到过 `id` 时，`Open` 会自动给这次请求补上 `Last-Event-ID` 头；
- 调用方自己设过 `Last-Event-ID` 则不覆盖，显式值优先；
- `LastEventID()` 可随时读取当前值，想自己掌控重连时机时用它。

### 浏览器

浏览器原生 `EventSource` 自带重连，直接用它即可：

```js
const es = new EventSource('/events')
es.onmessage = (e) => console.log(e.data)
es.addEventListener('update', (e) => console.log(e.data))
es.onerror = () => es.close()
```

### 解析行为

- 同一事件块的多个 `data:` 行用 `\n` 拼接，`"a\n\nb"` 原样还原，不会只剩最后一行；
- 字段值只去掉开头的一个空格（规范行为），不做 `TrimSpace`，payload 首尾空白是数据的一部分；
- 空行结束一个事件块；块内没有 `data:` 时不派发事件，所以服务端的 `: connected` 与心跳注释不会变成空事件；
- `id:` 直接覆盖 last event ID（空值即重置为空），没有 `id:` 的事件块沿用上一个值；
- `ctx` 取消会立即关闭连接、唤醒阻塞中的读，`Open` 返回 `ctx.Err()`；单行上限 10MB。

## 测试

```bash
go test -race -count=1 ./gsse/
```
