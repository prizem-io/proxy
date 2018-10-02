// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"
	uuid "github.com/satori/go.uuid"
)

type Local struct {
	servicesMu     sync.RWMutex
	servicesByName map[string]*ServiceNodes
	servicesByHost map[string]*ServiceNodes
}

func NewLocal() *Local {
	return &Local{
		servicesByName: make(map[string]*ServiceNodes, 20),
		servicesByHost: make(map[string]*ServiceNodes, 20),
	}
}

func (l *Local) Register(serviceInstance *api.ServiceInstance, node *api.Node) {
	l.servicesMu.Lock()
	defer l.servicesMu.Unlock()

	serviceNodes, ok := l.servicesByName[serviceInstance.Service]
	if !ok {
		serviceNodes = &ServiceNodes{
			Service: serviceInstance,
			Nodes:   []*api.Node{node},
		}
		l.servicesByName[serviceInstance.Service] = serviceNodes
	} else {
		found := false
		for i := range serviceNodes.Nodes {
			if serviceNodes.Nodes[i].ID == node.ID {
				serviceNodes.Nodes[i] = node
				found = true
				break
			}
		}
		if !found {
			serviceNodes.Nodes = append(serviceNodes.Nodes, node)
		}
	}

	host := node.Address.String()
	if host == "::1" {
		host = "127.0.0.1"
	}

	l.servicesByHost[host] = &ServiceNodes{
		Service: serviceInstance,
		Nodes:   []*api.Node{node},
	}
}

func (l *Local) GetServiceNodes(service string) (*ServiceNodes, error) {
	l.servicesMu.RLock()
	defer l.servicesMu.RUnlock()

	serviceNodes, ok := l.servicesByName[service]
	if !ok {
		return nil, proxy.ErrServiceUnavailable
	}
	return serviceNodes, nil
}

func (l *Local) GetSourceInstance(remoteAddr net.Addr, headers proxy.Headers) (*api.ServiceInstance, error) {
	host, _, err := net.SplitHostPort(remoteAddr.String())
	if err != nil {
		return nil, err
	}

	if host == "::1" {
		host = "127.0.0.1"
	}

	l.servicesMu.RLock()
	source, ok := l.servicesByHost[host]
	l.servicesMu.RUnlock()

	if ok {
		return source.Service, nil
	}

	return nil, nil
}

func (l *Local) HandleRegister(nodeID uuid.UUID, controlPlaneRESTURI string, ingressListenPort int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var serviceInstance api.ServiceInstance
		err := json.NewDecoder(r.Body).Decode(&serviceInstance)
		if err != nil {
			fmt.Fprintf(w, "ERROR")
			return // TODO
		}
		serviceInstance.Prepare()

		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			fmt.Fprintf(w, "ERROR")
			return // TODO
		}

		if host == "::1" {
			host = "127.0.0.1"
		}
		nodeAddress := net.ParseIP(host)

		if uuid.Equal(serviceInstance.ID, uuid.Nil) {
			serviceInstance.ID = uuid.NewV4()
		}

		l.Register(&serviceInstance, &api.Node{
			ID:         nodeID,
			Address:    nodeAddress,
			Geography:  "local",
			Datacenter: "local",
			Metadata: api.Metadata{
				"foo": "bar",
			},
			Services: []api.ServiceInstance{
				serviceInstance,
			},
		})

		serviceInstanceCopy := serviceInstance
		serviceInstanceCopy.Ports = api.Ports{
			{
				Port:     int32(ingressListenPort),
				Protocol: "HTTP/2",
				Secure:   true,
			},
		}
		node := api.Node{
			ID:         nodeID,
			Geography:  "us-east",
			Datacenter: "dc1",
			Metadata: api.Metadata{
				"foo": "bar",
			},
			Services: []api.ServiceInstance{
				serviceInstanceCopy,
			},
		}

		reqData, err := json.Marshal(&node)
		if err != nil {
			fmt.Fprintf(w, "ERROR")
			return // TODO
		}

		req, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/endpoints", controlPlaneRESTURI), bytes.NewBuffer(reqData))
		if err != nil {
			fmt.Fprintf(w, "ERROR")
			return // TODO
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintf(w, "ERROR")
			return // TODO
		}
		defer resp.Body.Close()

		if resp.StatusCode != 201 {
			fmt.Fprintf(w, "ERROR")
			return // TODO
		}

		fmt.Fprintf(w, "OK")
	}
}

func (l *Local) HandleInfo(w http.ResponseWriter, r *http.Request) {
	l.servicesMu.RLock()
	payload, err := json.Marshal(l.servicesByHost)
	l.servicesMu.RUnlock()
	if err != nil {
		fmt.Fprintf(w, "ERROR")
		return // TODO
	}

	w.Header().Add("Content-Type", "application/json")
	w.Write(payload)
}
