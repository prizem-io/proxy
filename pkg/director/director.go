// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package director

import (
	"math"
	"net"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"
	"github.com/prizem-io/routerstore"

	"github.com/prizem-io/proxy/pkg/discovery"
	"github.com/prizem-io/proxy/pkg/log"
)

type Director struct {
	logger            log.Logger
	pathInfo          PathInfoFn
	serviceChecker    ServiceChecker
	sourceInstance    SourceInstanceFn
	destinationNodes  DestinationNodesFn
	loadBalancer      LoadBalancer
	upstreams         *Upstreams
	dialers           UpstreamDialers
	dialer            proxy.Dialer
	connectionChecker ConnectionChecker
	middleware        []proxy.Middleware
}

type ProxyInfo struct {
	Service     *api.Service
	Operation   *api.Operation
	Source      *api.ServiceInstance
	Destination *api.ServiceInstance
	Node        *api.Node
	Port        *api.Port
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

type ServiceChecker func(proxyInfo *ProxyInfo) bool
type PathInfoFn func(method, path string) (*discovery.PathInfo, error)
type SourceInstanceFn func(remoteAddr net.Addr, headers proxy.Headers) (*api.ServiceInstance, error)
type DestinationNodesFn func(service string) (*discovery.ServiceNodes, error)
type ConnectionChecker func(conn net.Conn, pathInfo *discovery.PathInfo) error

type LoadBalancer func(proxyInfo *ProxyInfo, serviceNodes *discovery.ServiceNodes, nodes []*api.Node, upstreams *Upstreams, serviceChecker ServiceChecker) *api.Node

// AlwaysService is a ServiceChecker that always allows the request / upstream / service instance to be processed.
func AlwaysService(proxyInfo *ProxyInfo) bool {
	return true
}

func RoundRobin(proxyInfo *ProxyInfo, serviceNodes *discovery.ServiceNodes, nodes []*api.Node, _ *Upstreams, serviceChecker ServiceChecker) *api.Node {
	for i := 0; i < len(nodes); i++ {
		index := atomic.AddUint32(&serviceNodes.NodeIndex, 1)
		node := nodes[index%uint32(len(nodes))]

		proxyInfo.Destination = &node.Services[0]
		if serviceChecker(proxyInfo) {
			return node
		}
	}

	return nil
}

func LeastLoad(proxyInfo *ProxyInfo, serviceNodes *discovery.ServiceNodes, nodes []*api.Node, upstreams *Upstreams, serviceChecker ServiceChecker) *api.Node {
	leastStreams := math.MaxInt32
	var selectedNode *api.Node

	for _, node := range nodes {
		proxyInfo.Destination = &node.Services[0]
		if !serviceChecker(proxyInfo) {
			continue
		}

		if selectedNode == nil {
			selectedNode = node
			continue
		}
		key := upstreams.Key(proxyInfo.Service, node, &serviceNodes.Service.Ports[0])
		upstream, ok := upstreams.Get(key)

		if ok {
			streamCount := upstream.StreamCount()
			if streamCount < leastStreams {
				selectedNode = node
				leastStreams = streamCount
			}
		} else {
			// A node not yet connected to
			return node
		}
	}

	return selectedNode
}

func New(logger log.Logger,
	pathInfo PathInfoFn,
	serviceChecker ServiceChecker,
	sourceInstance SourceInstanceFn,
	destinationNodes DestinationNodesFn,
	upstreams *Upstreams,
	dialers UpstreamDialers,
	dialer proxy.Dialer,
	connectionChecker ConnectionChecker,
	loadBalancer LoadBalancer,
	middleware ...proxy.Middleware) *Director {
	return &Director{
		logger:            logger,
		pathInfo:          pathInfo,
		serviceChecker:    serviceChecker,
		sourceInstance:    sourceInstance,
		destinationNodes:  destinationNodes,
		loadBalancer:      loadBalancer,
		upstreams:         upstreams,
		dialers:           dialers,
		dialer:            dialer,
		connectionChecker: connectionChecker,
		middleware:        middleware,
	}
}

func (d *Director) Direct(conn net.Conn, headers proxy.Headers) (proxy.Target, error) {
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

	if d.connectionChecker != nil {
		err := d.connectionChecker(conn, pathInfo)
		if err != nil {
			d.logger.Error(err)
			return proxy.Target{}, err
		}
	}

	//fmt.Printf("service name = %s\n", pathInfo.Service.Name)

	source, err := d.sourceInstance(conn.RemoteAddr(), headers)
	if err != nil {
		return proxy.Target{}, err
	}

	proxyInfo := ProxyInfo{
		Service:   pathInfo.Service,
		Operation: pathInfo.Operation,
		Source:    source,
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

	node := d.loadBalancer(&proxyInfo, serviceNodes, nodes, d.upstreams, d.serviceChecker)
	if node == nil {
		return proxy.Target{}, proxy.ErrServiceUnavailable
	}
	proxyInfo.Destination = &node.Services[0]
	port := serviceNodes.Service.Ports[0]
	proxyInfo.Node = node
	proxyInfo.Port = &port

	upstreamKey := d.upstreams.Key(proxyInfo.Service, node, &port)
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
			return proxy.Target{}, errors.Errorf("no registered dialer for protocol %q", port.Protocol)
		}

		upstream, err = dial(proxyInfo.Source, proxyInfo.Service, node, &port, d.dialer)
		if err != nil {
			d.logger.Error(err)
			return proxy.Target{}, errors.Wrapf(err, "could not connect to service %q", proxyInfo.Service.Name)
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
		Info:        &proxyInfo,
	}, nil
}
