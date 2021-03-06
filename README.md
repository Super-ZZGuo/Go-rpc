# go-rpc

基于Go标准库`net/rpc`实现的简易RPC框架，实现了

- 自定义编/解码方式
- 支持**并发**和**异步**的高性能客户端
- 基于**反射**的服务发现功能(将结构体注册为服务
- 防止**服务挂死**，添加了**超时处理**功能
- 服务端和客户端 支持 `TCP`、`Unix`、`HTTP` 等多种传输协议；
- 客户端支持自定义负载均衡模式
  - 实现了**随机**、**轮询**两种负载均衡
- 简单的注册中心，支持**服务注册**、**心跳保活**等功能

详细实现可以[查看](https://github.com/Super-ZZGuo/Go-rpc/tree/main/Practice)
