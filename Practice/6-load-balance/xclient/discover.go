package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	// 随机
	RandomSelect SelectMode = iota
	// 轮询
	RoundRobinSelect
)

type Discovery interface {
	// 在注册中心更新服务列表
	Refresh() error
	// 手动更新服务列表
	Update(servers []string) error
	// 选择负载均衡模式
	Get(mode SelectMode) (string, error)
	// 返回所有实例
	GetAll() ([]string, error)
}

// 实现Discovery接口
var _ Discovery = (*MultiServersDiscovery)(nil)

// MultiServersDiscovery 不需要注册中心的手工维护的服务列表
type MultiServersDiscovery struct {
	// 随机数
	r *rand.Rand
	// 读写锁
	mu sync.RWMutex
	// 服务列表
	servers []string
	// 索引(轮询
	index int // record the selected position for robin algorithm
}

// Refresh 手工维护的服务列表 暂时不需要
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// Update 根据入参 更新服务列表
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

// Get 选择负载均衡模式
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	case RandomSelect:
		// 选择一个 0～n 内的随机服务
		return d.servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		// 取模确保数组越界
		s := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

// GetAll 返回服务列表
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}

// NewMultiServerDiscovery 初始化一个服务列表实例
func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		// 根据时间戳设定随机数
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	// 随机初始化索引
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}
