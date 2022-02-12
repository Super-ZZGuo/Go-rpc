package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type GobCodec struct {
	// 建立Socket链接实例
	conn io.ReadWriteCloser
	// 防止阻塞 带缓冲的Writer
	buf *bufio.Writer
	// 解码/反序列化
	dec *gob.Decoder
	// 编码/序列化
	enc *gob.Encoder
}

// Go小技巧 检查 结构体 是否实现 接口
var _ Codec = (*GobCodec)(nil)

// NewGobCodec 构造函数
func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		// 解码 -> 读取一次连接中的所有信息
		dec: gob.NewDecoder(conn),
		// 编码 -> 往一个新的buf写缓冲里写入内容
		enc: gob.NewEncoder(buf),
	}
}

// ReadHeader 获取 请求头
func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

// ReadBody 获取 请求体
func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		// 缓冲区写入
		_ = c.buf.Flush()
		// 错误则关闭这次连接
		if err != nil {
			_ = c.Close()
		}
	}()
	// 请求头 错误处理
	if err = c.enc.Encode(h); err != nil {
		log.Println("rpc: gob error encoding header:", err)
		return
	}
	// 请求体 错误处理
	if err = c.enc.Encode(body); err != nil {
		log.Println("rpc: gob error encoding body:", err)
		return
	}
	return
}

// Close 断开链接
func (c *GobCodec) Close() error {
	return c.conn.Close()
}
