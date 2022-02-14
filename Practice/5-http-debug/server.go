package gorpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"gorpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const Number = 0x1a2b3c

type Option struct {
	// 标记该请求为rpc请求
	Number int
	// 编解码方式
	CodecType codec.Type
	// 连接超时 默认10s
	ConnectTimeout time.Duration
	// 处理请求超时 默认0 表示不设限
	HandleTimeout time.Duration
}

// DefaultOption 默认选择为GobType
var DefaultOption = &Option{
	Number:    Number,
	CodecType: codec.GobType,
}

// Server 一次rpc服务
type Server struct {
	serviceMap sync.Map
}

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
	server.serveCodec(f(conn), &opt)
}

// invalidRequest 发生错误时候的 argv 占位符
var invalidRequest = struct{}{}

// serveCodec 编解码处理
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
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
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
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
	argv reflect.Value
	// 回复参数
	replyv reflect.Value
	mtype  *methodType
	svc    *service
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

func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	// 检查请求服务格式
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	// 根据dot划分 服务名.方法名
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]

	// svci -> 找到对应Service实例
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	// 在对应 Service实例中 找到对应 methodType
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// readRequest 读取请求
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	//
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}

	// 创建入参实例
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// 注意argvi的值类型为指针或值类型
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

// sendResponse 发送响应
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	// 这里上锁 保证响应的有序发送 防止其他goroutine也在往同一个缓冲区写入
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// handleRequest 处理请求
// 处理超时
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()

	// 一次处理 分为两个过程
	// 用于事件通信
	// TODO 可以设置为 缓存信道 防止timeout后协程阻塞无法关闭 造成的内存泄漏
	called := make(chan struct{})
	sent := make(chan struct{})

	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)

		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-called:
		<-sent
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
		// 如果为缓存信道，则可以将下面注释掉
		<-called
		<-sent
	}
}

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

// Register 在服务器中注册
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	// .LoadOrStore -> getOfDefault(Java map)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

// Register 以 DefaultServer 注册
func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

const (
	connected        = "200 Connected to Go RPC"
	defaultRPCPath   = "/gorpc"
	defaultDebugPath = "/debug/gorpc"
)

// ServeHTTP 实现 http.Handler 去接收RPC请求
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		// 设置请求头
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		// 405
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	// TODO 使用Hijack使  HTTP/1.1 来支持 GRPC 的 stream rpc
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

// HandleHTTP 在rpcPath上注册RPC消息的HTTP处理程序
//            在debugPath上注册调试处理程序
func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
	//  debugHTTP 实例绑定到地址 /debug/gorpc
	http.Handle(defaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
}

// HandleHTTP 默认服务器注册HTTP注册程序
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
