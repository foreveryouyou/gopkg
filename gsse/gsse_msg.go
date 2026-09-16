package gsse

import (
	"io"
	"strconv"
	"strings"
	"time"
)

var (
	fieldBytesData    = []byte("data: ")
	fieldBytesEvent   = []byte("event: ")
	fieldBytesRetry   = []byte("retry: ")
	fieldBytesID      = []byte("id: ")
	fieldBytesComment = []byte(": ")

	newline = []byte{'\n'}
)

// Message 表示一条 SSE 消息。
type Message struct {
	ID        *string       `json:"id"`        // 消息ID
	Type      *string       `json:"type"`      // 消息EventType
	Data      string        `json:"data"`      // 消息体
	Retry     time.Duration `json:"retry"`     // 重试
	IsComment bool          `json:"isComment"` // 是否是注释消息
}

type MsgOpt func(msg *Message)

func NewMessage(data string, opts ...MsgOpt) (msg *Message) {
	msg = &Message{
		Data: data,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(msg)
	}
	return msg
}

func NewCommentMessage(data string) (msg *Message) {
	msg = &Message{
		Data:      data,
		IsComment: true,
	}
	return
}

func WithID(id string) MsgOpt {
	return func(msg *Message) {
		msg.ID = &id
	}
}

func WithType(eventType string) MsgOpt {
	return func(msg *Message) {
		msg.Type = &eventType
	}
}

func WithRetry(retry time.Duration) MsgOpt {
	return func(msg *Message) {
		msg.Retry = retry
	}
}

// -------------------------------------------------------------------------------------

// String 返回消息的 SSE 线路格式，可直接用于落库 / 回放。
func (p *Message) String() string {
	if p == nil {
		return ""
	}
	s := strings.Builder{}
	_, _ = p.WriteTo(&s)
	return s.String()
}

// WriteTo 按 SSE 规范把消息编码写入 w。
//
// 与旧实现的区别：data / comment 的值会先按 \r\n、\r、\n 规范化为多行，再逐行输出，
// 因此值里包含换行时既不会截断内容（只发出第一行），也不会凭空多出一条消息（\n\n 破帧）。
// 空值会输出一条 "data: " 行，等价于一个 data 为空的事件。
func (p *Message) WriteTo(w io.Writer) (n int64, err error) {
	if p == nil || w == nil {
		return 0, nil
	}

	// add 累加写入字节数，并保留第一个出现的错误。
	add := func(c int, e error) {
		n += int64(c)
		if err == nil {
			err = e
		}
	}

	if p.ID != nil && *p.ID != "" {
		add(writeFieldLine(w, fieldBytesID, sanitizeFieldValue(*p.ID)))
		if err != nil {
			return
		}
	}

	if p.Type != nil && *p.Type != "" {
		add(writeFieldLine(w, fieldBytesEvent, sanitizeFieldValue(*p.Type)))
		if err != nil {
			return
		}
	}

	if p.Retry > 0 {
		if millis := p.Retry.Milliseconds(); millis > 0 {
			var buf [20]byte // int64 十进制最长 19 位 + 1
			add(writeRawFieldLine(w, fieldBytesRetry, strconv.AppendInt(buf[:0], millis, 10)))
			if err != nil {
				return
			}
		}
	}

	fieldName := fieldBytesData
	if p.IsComment {
		fieldName = fieldBytesComment
	}
	for _, line := range splitDataLines(p.Data) {
		add(writeFieldLine(w, fieldName, line))
		if err != nil {
			return
		}
	}

	// 一个事件以空行结束
	add(writeBytes(w, newline))
	return
}

// -------------------------------------------------------------------------------------

// writeBytes 写一段原始字节。
func writeBytes(w io.Writer, b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	return w.Write(b)
}

func writeFieldLine(w io.Writer, prefix []byte, value string) (int, error) {
	return writeRawFieldLine(w, prefix, []byte(value))
}

// writeRawFieldLine 输出 "prefix + value + \n" 一行。
// 注意 value 为空时也必须写出（SSE 里 "data: " 是一条合法的空 data 行），所以不能跳过空 chunk。
func writeRawFieldLine(w io.Writer, prefix, value []byte) (int, error) {
	n, err := w.Write(prefix)
	if err != nil {
		return n, err
	}

	m, err := writeBytes(w, value)
	n += m
	if err != nil {
		return n, err
	}

	m, err = w.Write(newline)
	return n + m, err
}

// splitDataLines 把 data 规范化成行序列：\r\n 与 \r 统一为 \n 后按 \n 拆分。
// 空串会返回单条空行（对应一条 "data: " 行），保持与客户端解码语义一致。
func splitDataLines(s string) []string {
	if !strings.ContainsAny(s, "\r\n") {
		return []string{s}
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return strings.Split(s, "\n")
}

// sanitizeFieldValue 用于 id / event 这类单行字段，避免值中的换行破坏帧结构。
func sanitizeFieldValue(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ").Replace(s)
}

func (p *Message) Map() (m map[string]any) {
	m = make(map[string]any)
	if p.ID != nil {
		m["id"] = *p.ID
	}
	if p.Type != nil {
		m["type"] = *p.Type
	}
	m["data"] = p.Data
	if p.Retry > 0 {
		m["retry"] = p.Retry
	}
	if p.IsComment {
		m["isComment"] = true
	}
	return
}
