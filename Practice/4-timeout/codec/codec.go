package codec

import "io"

type Header struct {
	// rpc调用方法名 例: Service.Method
	ServiceMethod string
	// rpc请求序号 便于服务端按顺序处理请求
	Seq uint64
	// 错误信息
	Error string
}

// Codec 消息编解码接口
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

// NewCodecFunc 抽象 编解码构造函数
type NewCodecFunc func(io.ReadWriteCloser) Codec

// Type 编解码类型
type Type string

// 定义两个编解码器类型
const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
