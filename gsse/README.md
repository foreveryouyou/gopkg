# gsse

只依赖标准库 `net/http` 的 SSE（Server-Sent Events）服务端实现。

## 设计

**一个连接对应一个 `SSEServer` 实例**：

- 生产者调用 `SendMessage` 把消息入队；
- `Serve` 在唯一一个 goroutine 中消费队列并写回连接；
- `Stop` / `Done` 关闭队列，`Serve` 会把已入队的消息**排空写完**再退出，不丢最后一条事件。

## 快速开始

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

## 主动结束

```go
sse.Stop()      // 只发结束信号，不等待
<-sse.Done()    // 结束并等待收尾完成（已入队消息全部写出后才返回）
```

`Cause()` 返回结束原因：`CancelErrStop`（Stop/Done）或 `CancelErrDone`（Serve 自身退出）。

## API

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

## 消息选项

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

## 错误

| 错误 | 含义 |
| --- | --- |
| `ErrAlreadyServed` | 同一实例被 `Serve` 了第二次（实例只能 Serve 一次） |
| `ErrChannelClosed` | 服务已结束，`SendMessage` 立即失败，而不是阻塞 |
| `ErrSendTimeout` | 队列写满且超过等待上限（默认 5s），消息未入队 |
| `ErrNilWriter` | `Serve` 收到 nil 的 `http.ResponseWriter` |

## 内置行为

- **立即建连**：`Serve` 一开始就下发响应头和一条 `: connected` 注释并 flush，客户端无需等到首条业务消息。
- **心跳**：每 10s 发送一条注释消息（默认 `ping`），防止中间代理因空闲断连。
- **反代友好**：自动设置 `X-Accel-Buffering: no`，关闭 nginx 等代理的响应缓冲。
- **生产端不会被永久阻塞**：队列默认容量 100，写满时最长等 5s 后返回 `ErrSendTimeout`。

## 客户端

浏览器原生支持：

```js
const es = new EventSource('/events')
es.onmessage = (e) => console.log(e.data)
es.addEventListener('update', (e) => console.log(e.data))
es.onerror = () => es.close()
```

## 测试

```bash
go test -race -count=1 ./gsse/
```
