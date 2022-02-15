package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

// GoRegistryDiscovery
// 嵌套MultiServersDiscovery，提高复用率
type GoRegistryDiscovery struct {
	*MultiServersDiscovery
	// 注册中心地址
	registry string
	// 注册中心过期时间
	timeout time.Duration
	// 最后从注册中心更新服务列表的时间
	// 默认10s过期
	lastUpdate time.Time
}

const defaultUpdateTimeout = time.Second * 10

// NewGoRegistryDiscovery 初始化
func NewGoRegistryDiscovery(registerAddr string, timeout time.Duration) *GoRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &GoRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:              registerAddr,
		timeout:               timeout,
	}
	return d
}

// Update 根据入参更新 服务列表
func (d *GoRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

// Refresh 超时 自动更新服务列表
func (d *GoRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	// 返回可用服务列表
	servers := strings.Split(resp.Header.Get("X-Gorpc-Servers"), ",")
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.servers = append(d.servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

// Get 根据负载均衡模式 返回一个可用服务实例
func (d *GoRegistryDiscovery) Get(mode SelectMode) (string, error) {
	// 先调用 Refresh 确保服务列表没有过期
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}

// GetAll 返回全部服务实例
func (d *GoRegistryDiscovery) GetAll() ([]string, error) {
	// 先调用 Refresh 确保服务列表没有过期
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}
