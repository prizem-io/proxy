package director

import (
	"net"
	"strconv"
	"sync"

	"github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"
)

type ConnectionSharingMode int

const (
	PerNode ConnectionSharingMode = iota
	PerService
)

type Upstreams struct {
	sharingMode ConnectionSharingMode
	upstreamsMu sync.RWMutex
	upstreams   map[string]proxy.Upstream
}

func NewUpstreams(sharingMode ConnectionSharingMode, size int) *Upstreams {
	return &Upstreams{
		sharingMode: sharingMode,
		upstreams:   make(map[string]proxy.Upstream, size),
	}
}

func (u *Upstreams) Key(service *api.Service, node *api.Node, port *api.Port) string {
	if u.sharingMode == PerService {
		return service.Name + "|" + net.JoinHostPort(node.Address.String(), strconv.Itoa(int(port.Port))) + ":" + port.Protocol
	}
	return net.JoinHostPort(node.Address.String(), strconv.Itoa(int(port.Port))) + ":" + port.Protocol
}

func (u *Upstreams) Get(key string) (upstream proxy.Upstream, ok bool) {
	u.upstreamsMu.RLock()
	defer u.upstreamsMu.RUnlock()
	upstream, ok = u.upstreams[key]
	return
}

func (u *Upstreams) Put(key string, upstream proxy.Upstream) {
	u.upstreamsMu.Lock()
	defer u.upstreamsMu.Unlock()
	u.upstreams[key] = upstream
}

func (u *Upstreams) Remove(key string) {
	u.upstreamsMu.Lock()
	defer u.upstreamsMu.Unlock()
	delete(u.upstreams, key)
}
