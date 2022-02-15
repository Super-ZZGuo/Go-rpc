package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// GoRegistry 注册中心
type GoRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_gorpc_/registry"
	defaultTimeout = time.Minute * 5
)

// New 创建一个带timeout的注册中心实例
func New(timeout time.Duration) *GoRegistry {
	return &GoRegistry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultGoRegister = New(defaultTimeout)

// 添加服务实例,服务已存在则更新
func (r *GoRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		// 更新时间
		s.start = time.Now()
	}
}

// 返回可用服务列表
func (r *GoRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		// 未超时服务
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			// 删除 超时服务
			delete(r.servers, addr)
		}
	}
	// 根据服务名 排序
	sort.Strings(alive)
	return alive
}

//  注册中心信息采用HTTP提供服务 /_gorpc_/registry
func (r *GoRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	// 返回可用服务列表
	case "GET":
		w.Header().Set("X-Gorpc-Servers", strings.Join(r.aliveServers(), ","))
	// 添加服务实例/发送心跳
	case "POST":
		addr := req.Header.Get("X-Gorpc-Server")
		if addr == "" {
			// 500
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		// 405
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleHTTP 注册HTTP处理程序
func (r *GoRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultGoRegister.HandleHTTP(defaultPath)
}

// Heartbeat 定时向服务中心发送心跳
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// 发送心跳周期默认比 注册中心过期时间少1min
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	// 定时器
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Gorpc-Server", addr)

	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
