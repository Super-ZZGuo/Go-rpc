package main

import (
	"encoding/json"
	"fmt"
	"gorpc"
	"gorpc/codec"
	"log"
	"net"
	"time"
)

func startServer(addr chan string) {
	// 服务端监听一个端口
	l, err := net.Listen("tcp", ":9097")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	// 信道中加入网络地址 确保连接成功
	addr <- l.Addr().String()
	gorpc.Accept(l)
}

// main 简单模拟一个客户端发起请求
func main() {
	log.SetFlags(0)
	addr := make(chan string)
	// 协程开启服务端服务
	go startServer(addr)

	// 使用net.Dial拨号 模拟一个简单的rpc请求
	conn, _ := net.Dial("tcp", <-addr)
	defer func() { _ = conn.Close() }()

	// 确保连接成功后 再发起rpc请求
	// 隔离协议交换阶段与RPC消息阶段，防止客户端发起过多rpc信息积压
	// 如发送 Option|Header|Body|Header|Body
	// 造成的后续 |Body|Header|Body 解码失败
	time.Sleep(2 * time.Second)
	// 发送协议交换参数
	_ = json.NewEncoder(conn).Encode(gorpc.DefaultOption)
	cc := codec.NewGobCodec(conn)
	// 发送 & 接受请求
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           uint64(i),
		}
		_ = cc.Write(h, fmt.Sprintf("geerpc req %d", h.Seq))
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
