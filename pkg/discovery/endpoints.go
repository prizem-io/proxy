// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package discovery

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"
	uuid "github.com/satori/go.uuid"

	"github.com/prizem-io/proxy/pkg/log"
)

type (
	ServiceNodes struct {
		Service   *api.ServiceInstance
		Nodes     []*api.Node
		NodeIndex uint32
	}

	Endpoints struct {
		logger       log.Logger
		url          string
		serivceNodes unsafe.Pointer
		sourceNodes  unsafe.Pointer
		nodeServices unsafe.Pointer

		mu      sync.RWMutex
		version int64
	}
)

func NewEndpoints(logger log.Logger, baseURL string) *Endpoints {
	serviceNodes := map[string]*ServiceNodes{}
	sourceNodes := map[uuid.UUID]*ServiceNodes{}
	nodeServices := map[uuid.UUID]*api.Node{}
	e := Endpoints{
		logger: logger,
		url:    fmt.Sprintf("%s/v1/endpoints", baseURL),
	}
	atomic.StorePointer(&e.serivceNodes, unsafe.Pointer(&serviceNodes))
	atomic.StorePointer(&e.sourceNodes, unsafe.Pointer(&sourceNodes))
	atomic.StorePointer(&e.nodeServices, unsafe.Pointer(&nodeServices))
	return &e
}

func (e *Endpoints) RequestCatalog() error {
	rs, err := http.Get(e.url)
	if err != nil {
		return err
	}
	defer rs.Body.Close()

	etag := rs.Header.Get("ETag")
	var version int64
	if etag != "" {
		version, err = strconv.ParseInt(etag, 10, 63)
		if err == nil {
			var ri int64
			e.mu.RLock()
			ri = e.version
			e.mu.RUnlock()
			if version == ri {
				return nil
			}
		}
	}

	bodyBytes, err := ioutil.ReadAll(rs.Body)
	if err != nil {
		return err
	}

	var catalog api.EndpointInfo
	err = json.Unmarshal(bodyBytes, &catalog)
	if err != nil {
		return err
	}

	catalog.Prepare()

	e.StoreEndpoints(version, catalog.Nodes)

	return nil
}

func (e *Endpoints) StoreEndpoints(version int64, nodes []api.Node) bool {
	serviceNodes := make(map[string]*ServiceNodes)
	sourceNodes := make(map[uuid.UUID]*ServiceNodes)
	nodeServices := make(map[uuid.UUID]*api.Node)

	for i := range nodes {
		node := &nodes[i]
		for j := range node.Services {
			service := &node.Services[j]
			sn, ok := serviceNodes[service.Service]
			if !ok {
				sn = &ServiceNodes{
					Service:   service,
					Nodes:     make([]*api.Node, 0, 10),
					NodeIndex: math.MaxUint32,
				}
				serviceNodes[service.Service] = sn
			}
			sn.Nodes = append(sn.Nodes, node)

			source, ok := sourceNodes[service.ID]
			if !ok {
				source = &ServiceNodes{
					Service:   service,
					Nodes:     make([]*api.Node, 0, 10),
					NodeIndex: math.MaxUint32,
				}
				sourceNodes[service.ID] = sn
			}
			source.Nodes = append(source.Nodes, node)
		}

		nodeServices[node.ID] = node
	}
	stored := false

	e.mu.Lock()
	if version != e.version {
		e.logger.Infof("Storing endpoints for updated index %d", version)
		atomic.StorePointer(&e.serivceNodes, unsafe.Pointer(&serviceNodes))
		atomic.StorePointer(&e.sourceNodes, unsafe.Pointer(&sourceNodes))
		atomic.StorePointer(&e.nodeServices, unsafe.Pointer(&nodeServices))
		e.version = version
		stored = true
	}
	e.mu.Unlock()

	return stored
}

func (e *Endpoints) RefreshLoop(duration time.Duration, stop chan struct{}) {
	t := time.Tick(duration)
	for {
		select {
		case <-t:
			err := e.RequestCatalog()
			if err != nil {
				e.logger.Errorf("Error requesting catalog: %v", err)
			}
		case <-stop:
			break
		}
	}
}

func (e *Endpoints) GetNodeServices(nodeID uuid.UUID) (*api.Node, error) {
	ptr := atomic.LoadPointer(&e.nodeServices)
	nodes := *(*map[uuid.UUID]*api.Node)(ptr)
	nodeServices, ok := nodes[nodeID]
	if !ok {
		return nil, errors.Errorf("could not find node %q", nodeID)
	}

	return nodeServices, nil
}

func (e *Endpoints) GetServiceNodes(service string) (*ServiceNodes, error) {
	ptr := atomic.LoadPointer(&e.serivceNodes)
	routes := *(*map[string]*ServiceNodes)(ptr)
	serviceNodes, ok := routes[service]
	if !ok {
		return nil, errors.Errorf("could not find service %q", service)
	}

	return serviceNodes, nil
}

func (e *Endpoints) GetSourceInstance(remoteAddr net.Addr, headers proxy.Headers) (*api.ServiceInstance, error) {
	sourceIDStr := headers.ByName("x-source-instance-id")
	if sourceIDStr == "" {
		return nil, nil
	}

	sourceID, err := uuid.FromString(sourceIDStr)
	if err != nil {
		return nil, err
	}

	ptr := atomic.LoadPointer(&e.sourceNodes)
	sources := *(*map[uuid.UUID]*ServiceNodes)(ptr)
	serviceNodes, ok := sources[sourceID]
	if !ok {
		return nil, nil
	}

	return serviceNodes.Service, nil
}
