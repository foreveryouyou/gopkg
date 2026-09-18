# gopkg

Go 工具集，每个包各自独立，按需单独引入即可。

## 安装

```bash
go get github.com/foreveryouyou/gopkg/gsse
```

只依赖标准库（`net/http`），无第三方依赖。要求 Go 1.26+。

## 使用

```go
import "github.com/foreveryouyou/gopkg/gsse"
```

### 一个最小示例

服务端：

```go
http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
	sse := gsse.NewSSEServer()          // 一个连接一个实例
	go func() {
		_ = sse.SendMessage("hello", gsse.WithID("1"), gsse.WithType("message"))
		<-sse.Done()                    // 想等待收尾完成就 <-sse.Done()，只想结束就 sse.Stop()
	}()
	_ = sse.Serve(w, r)                 // 阻塞到连接结束
})
```

客户端：

```go
c := gsse.NewSSEClient(func(e gsse.Event) { println(e.Type, e.Data) })
req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:8080/events", nil)
_ = c.Open(ctx, req)                    // 断线后重新构造 req 再 Open，Last-Event-ID 会自动带上
```

## 包列表

| 包 | 说明 |
| --- | --- |
| [gsse](./gsse/README.md) | 只依赖标准库的 SSE（Server-Sent Events）实现，含服务端 `SSEServer` 与客户端 `SSEClient` |

更多用法见各包下的 README。
