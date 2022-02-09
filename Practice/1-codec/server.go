package gorpc

import (
	"encoding/json"
	"fmt"
	"gorpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const Number = 0x1a2b3c

type Option struct {
	// 标记该请求为rpc请求
	Number int
	// 编解码方式
	CodecType codec.Type
}

// DefaultOption 默认选择为GobType
var DefaultOption = &Option{
	Number:    Number,
	CodecType: codec.GobType,
}

// Server 一次rpc服务
type Server struct{}

// NewServer 构造函数
func NewServer() *Server {
	return &Server{}
}

// ServeConn 处理一次rpc连接下的请求 直到客户端断开请求
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	// 反序列化得到Option实例
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	// 检查 Number值
	if opt.Number != Number {
		log.Printf("rpc server: invalid magic number %x", opt.Number)
		return
	}
	// 检查 编码格式
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}

	server.serveCodec(f(conn))
}

// serveCodec 编解码处理
func (server *Server) serveCodec(cc codec.Codec) {
	// 互斥锁 确保一个respone完整的发出
	sending := new(sync.Mutex)
	// 用于同步 等到所有请求处理完
	wg := new(sync.WaitGroup)

	for {
		// 1.读取请求
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				// 请求无法恢复 直接断开连接
				break
			}
			req.h.Error = err.Error()
			// 3.回复请求
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		// 2.处理请求 计数器+1
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg)
	}
	// 阻塞 直到请求处理完
	wg.Wait()
	_ = cc.Close()
}

// request 存储 请求信息
type request struct {
	// 请求头
	h *codec.Header
	// 请求参数
	argv, replyv reflect.Value
}

// readRequestHeader 读取请求头
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// readRequest 读取请求
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	// TODO: 还未知请求的 argv 参数
	// 先当作 string
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

// sendResponse 发送响应
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	// 这里上锁
	// 保证响应的有序发送 防止其他goroutine也在往同一个缓冲区写入
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	// TODO: 应该通过rpc请求的 方法 来构建一个正确的 响应
	// 这里还未实现客户端 就打印出 argv参数 并发送一个"hello"的信息

	// 处理完后 计数器-1
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("gorpc resp %d", req.h.Seq))
	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}

// invalidRequest 发生错误时候的 argv 占位符
var invalidRequest = struct{}{}

// DefaultServer *Server的默认实例
var DefaultServer = NewServer()

// Accept 接受server请求
func (server *Server) Accept(lis net.Listener) {
	// 循环等待socket连接建立
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		// 开启 子协程 处理连接请求
		go server.ServeConn(conn)
	}
}

// Accept 包装Accept函数 方便使用
// 一次启动服务如下
// lis, _ := net.Listen("tcp", ":9999")
// gorpc.Accept(lis)
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }
