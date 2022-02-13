package gorpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gorpc/codec"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Call 一次rpc调用所需要的信息
type Call struct {
	// 序列号
	Seq uint64
	// 请求方法
	ServiceMethod string
	// 请求参数
	Args interface{}
	// 方法的返回值
	Reply interface{}
	// 错误信息
	Error error
	// 调用后的回调
	Done chan *Call
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	// 消息编/解码器
	cc codec.Codec
	// 发起连接前的确认(请求类型/编码方式）
	opt *Option
	// 保证Client并发时可用性
	sending sync.Mutex
	// 每个请求的消息头
	header codec.Header
	// 保证内部服务的有序性
	mu sync.Mutex
	// 发送请求的编号
	seq uint64
	// 存储未发送完的请求 k:V -> 编号:请求实例
	pending map[uint64]*Call
	// 是否关闭rpc调用(用户正常关闭）
	closing bool
	// 服务停止(用于非正常closing）
	shutdown bool
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// Close 关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// IsAvailable 确保client服务正常前提
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// registerCall 客户端注册rpc请求
func (client *Client) registerCall(call *Call) (uint64, error) {
	// 方法内部上锁 防止并发问题 -> 该方法被其他client调用
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	// 序号++
	client.seq++
	return call.Seq, nil
}

// removeCall 客户端移除rpc请求
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// terminateCalls rpc请求错误
// defer处理顺序: client.mu.Unlock -> client.sending.Unlock
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	// 将所有错误信息通知等待处理中的call
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// send 请求发送
func (client *Client) send(call *Call) {
	// 加锁确保请求信息发送完整
	client.sending.Lock()
	defer client.sending.Unlock()

	// 先注册请求信息
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 准备请求头
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 编码 发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 对外暴露给用户的RPC调用接口
// 异步接口 返回Call实例
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	// 构造一个Call请求
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	// 请求发送
	// TODO 此处的send是同步等待的
	// sending.Lock()
	client.send(call)
	return call
}

// Call 封装Go
// 同步接口 call.Done，等待响应返回
// 处理超时
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	//TODO chan数量为1 保证同步
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done

	select {
	//TODO 提供一个供用户自定义的 具备超时检测能力的context对象来控制
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
	return call.Error
}

// receive 接收响应
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			//TODO call不存在 可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了？
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			// call存在 但是服务端处理出错
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			// 服务端处理正常
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	client.terminateCalls(err)
}

// NewClient 创建一个客户端实例
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	// 发送 option 编码给服务端
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return NewClientCodec(f(conn), opt), nil
}

func NewClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	// 开启一个协程 receive响应
	go client.receive()
	return client
}

type clientResult struct {
	client *Client
	err    error
}

// dialTimeout Dial外壳
// 超时处理
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 将net.Dial 替换为 net.DialTimeout
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// defer 关闭连接
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	// 创建客户端超时
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

// parseOptions 验证Options信息编码信息
func parseOptions(opts ...*Option) (*Option, error) {
	// 用户输入的Options信息有误时
	// 返回默认的DefaultOption
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.Number = DefaultOption.Number
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// Dial 传入服务端地址
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}
