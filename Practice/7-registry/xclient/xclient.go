package xclient

import (
	"context"
	. "gorpc"
	"io"
	"reflect"
	"sync"
)

// XClient 支持负载均衡的客户端
type XClient struct {
	// 服务发现实例
	d Discovery
	// 负载均衡模式
	mode SelectMode
	// 协议选项
	opt *Option
	mu  sync.Mutex // protect following
	// 缓存： 复用socket连接 保存创建好的Client实例
	clients map[string]*Client
}

var _ io.Closer = (*XClient)(nil)

// NewXClient 初始化负载均衡客户端
func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{
		d:       d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*Client)}
}

func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, client := range xc.clients {
		//TODO I have no idea how to deal with error, just ignore it.
		_ = client.Close()
		delete(xc.clients, key)
	}
	return nil
}

// dial 复用Client
func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	// 检查是否有缓存的client
	// 有则检查是否可用
	client, ok := xc.clients[rpcAddr]
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}
	// 没有 则新建 并添加进缓存
	if client == nil {
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client
	}
	return client, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	// 调用服务
	return client.Call(ctx, serviceMethod, args, reply)
}

// Call 封装call()
func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// Broadcast 广播服务
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	// 并发 广播
	var mu sync.Mutex
	var e error

	replyDone := reply == nil // if reply is nil, don't need to set value
	// 确保有错误发生的时候 快速失败
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			// 如果调用成功，则返回其中一个的结果
			err := xc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			// 如果任意一个实例发生错误，则返回其中一个错误
			if err != nil && e == nil {
				e = err
				cancel()
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
