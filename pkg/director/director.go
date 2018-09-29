// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package director

import (
	"crypto/tls"
	"math"
	"net"
	"strconv"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"
	"github.com/prizem-io/routerstore"

	"github.com/prizem-io/proxy/pkg/discovery"
	"github.com/prizem-io/proxy/pkg/log"
)

type Director struct {
	logger           log.Logger
	pathInfo         PathInfoFn
	sourceInstance   SourceInstanceFn
	destinationNodes DestinationNodesFn
	loadBalancer     LoadBalancer
	upstreams        *Upstreams
	dialers          proxy.UpstreamDialers
	tlsConfig        *tls.Config
	middleware       []proxy.Middleware
}

type ProxyInfo struct {
	Service     *api.Service
	Operation   *api.Operation
	Source      *api.ServiceInstance
	Destination *api.ServiceInstance
	APIKey      string
	Version     string
	Authorization
}

type Authorization struct {
	Principal string
	Audiences []string
	Presenter string
	Claims    map[string]string
}

type PathInfoFn func(method, path string) (*discovery.PathInfo, error)
type SourceInstanceFn func(remoteAddr net.Addr, headers proxy.Headers) (*api.ServiceInstance, error)
type DestinationNodesFn func(service string) (*discovery.ServiceNodes, error)

type LoadBalancer func(serviceNodes *discovery.ServiceNodes, nodes []*api.Node, upstreams *Upstreams) *api.Node

func RoundRobin(serviceNodes *discovery.ServiceNodes, nodes []*api.Node, _ *Upstreams) *api.Node {
	index := atomic.AddUint32(&serviceNodes.NodeIndex, 1)
	node := nodes[index%uint32(len(nodes))]
	return node
}

func LeastLoad(serviceNodes *discovery.ServiceNodes, nodes []*api.Node, upstreams *Upstreams) *api.Node {
	leastStreams := math.MaxInt32
	leastIndex := 0
	for i := range nodes {
		key := upstreams.Key(nodes[i], &serviceNodes.Service.Ports[0])
		upstream, ok := upstreams.Get(key)

		if ok {
			streamCount := upstream.StreamCount()
			if streamCount < leastStreams {
				leastIndex = i
			}
		} else {
			// A node not yet connected to
			leastIndex = i
		}
	}

	return nodes[leastIndex]
}

func New(logger log.Logger,
	pathInfo PathInfoFn,
	sourceInstance SourceInstanceFn,
	destinationNodes DestinationNodesFn,
	upstreams *Upstreams,
	dialers proxy.UpstreamDialers,
	tlsConfig *tls.Config,
	loadBalancer LoadBalancer,
	middleware ...proxy.Middleware) *Director {
	return &Director{
		logger:           logger,
		pathInfo:         pathInfo,
		sourceInstance:   sourceInstance,
		destinationNodes: destinationNodes,
		loadBalancer:     loadBalancer,
		upstreams:        upstreams,
		dialers:          dialers,
		tlsConfig:        tlsConfig,
		middleware:       middleware,
	}
}

func (d *Director) Direct(remoteAddr net.Addr, headers proxy.Headers) (proxy.Target, error) {
	method := headers.ByName(":method")
	path := headers.ByName(":path")

	//fmt.Printf("method = %s; path = %s\n", method, path)

	pathInfo, err := d.pathInfo(method, path)
	if err != nil {
		if err == routerstore.ErrNotFound {
			return proxy.Target{}, proxy.ErrNotFound
		}
		return proxy.Target{}, err
	}

	//fmt.Printf("service name = %s\n", info.Service.Name)

	source, err := d.sourceInstance(remoteAddr, headers)
	if err != nil {
		return proxy.Target{}, err
	}

	serviceNodes, err := d.destinationNodes(pathInfo.Service.Name)
	if err != nil {
		return proxy.Target{}, err
	}

	// Use selectors to filter on desired service tags.
	nodes := serviceNodes.Nodes[:0]
	for _, node := range serviceNodes.Nodes {
		for _, service := range node.Services {
			valid := true
			for _, selector := range pathInfo.Service.Selectors {
				if _, ok := service.LabelSet[selector]; !ok {
					valid = false
					break
				}
			}
			if valid {
				nodes = append(nodes, node)
			}
		}
	}

	if len(nodes) == 0 {
		return proxy.Target{}, proxy.ErrServiceUnavailable
	}

	node := d.loadBalancer(serviceNodes, nodes, d.upstreams)
	port := serviceNodes.Service.Ports[0]

	upstreamKey := d.upstreams.Key(node, &port)
	upstream, ok := d.upstreams.Get(upstreamKey)
	//ok = false // TESTING

	var _middleware [25]proxy.Middleware
	middleware := _middleware[:0]
	middleware = append(middleware, d.middleware...)
	if serivceMiddleware, ok := pathInfo.Service.Middleware.([]proxy.Middleware); ok {
		middleware = append(middleware, serivceMiddleware...)
	}
	if operationMiddleware, ok := pathInfo.Operation.Middleware.([]proxy.Middleware); ok {
		middleware = append(middleware, operationMiddleware...)
	}

	if !ok {
		d.logger.Infof("Connecting to upstream %s", upstreamKey)
		dial, ok := d.dialers.ForName(port.Protocol)
		if !ok {
			return proxy.Target{}, errors.Errorf("Unknown upstream %s", port.Protocol)
		}

		url := net.JoinHostPort(node.Address.String(), strconv.Itoa(int(port.Port)))

		var tlsConfig *tls.Config
		if port.Secure {
			tlsConfig = d.tlsConfig
		}

		upstream, err = dial(url, tlsConfig)
		if err != nil {
			return proxy.Target{}, err
		}

		d.upstreams.Put(upstreamKey, upstream)

		if upstream.IsServed() {
			go func(upstream proxy.Upstream, upstreamKey string) {
				err := upstream.Serve()
				d.logger.Infof("Disconnected from upstream %s", upstreamKey)
				if err != nil {
					d.logger.Errorf("Upstream.Serve: %v", err)
				}
				d.upstreams.Remove(upstreamKey)
			}(upstream, upstreamKey)
		}
	}

	return proxy.Target{
		Upstream:    upstream,
		Middlewares: middleware,
		Info: &ProxyInfo{
			Service:     pathInfo.Service,
			Operation:   pathInfo.Operation,
			Source:      source,
			Destination: serviceNodes.Service,
		},
	}, nil
}
