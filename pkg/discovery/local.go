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
	"net/url"
	"strings"
	"sync"
	"unicode"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"
	"github.com/prizem-io/proxy/pkg/log"
	"github.com/satori/go.uuid"
)

type Local struct {
	logger              log.Logger
	nodeID              uuid.UUID
	controlPlaneRESTURI string
	ingressListenPort   int

	servicesMu     sync.RWMutex
	servicesByName map[string]*ServiceNodes
	servicesByHost map[string]*ServiceNodes
}

var ErrBadResponse = errors.New("non-OK response")

func NewLocal(logger log.Logger, nodeID uuid.UUID, controlPlaneRESTURI string, ingressListenPort int) *Local {
	return &Local{
		logger:              logger,
		nodeID:              nodeID,
		controlPlaneRESTURI: controlPlaneRESTURI,
		ingressListenPort:   ingressListenPort,
		servicesByName:      make(map[string]*ServiceNodes, 20),
		servicesByHost:      make(map[string]*ServiceNodes, 20),
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

func (l *Local) HandleRegister(w http.ResponseWriter, r *http.Request) {
	var serviceInstance api.ServiceInstance
	err := json.NewDecoder(r.Body).Decode(&serviceInstance)
	if err != nil {
		fmt.Fprintf(w, "ERROR")
		return // TODO
	}
	serviceInstance.Prepare()

	host := r.Header.Get("X-Target-Host")
	if host == "" {
		host, _, err = net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			fmt.Fprintf(w, "ERROR")
			return // TODO
		}
	}

	if host == "::1" {
		host = "127.0.0.1"
	}
	nodeAddress := net.ParseIP(host)

	if uuid.Equal(serviceInstance.ID, uuid.Nil) {
		serviceInstance.ID = uuid.NewV4()
	}

	l.Register(&serviceInstance, &api.Node{
		ID:         l.nodeID,
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
			Port:     int32(l.ingressListenPort),
			Protocol: "HTTP/2",
			Secure:   true,
		},
	}
	node := api.Node{
		ID:         l.nodeID,
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

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/endpoints", l.controlPlaneRESTURI), bytes.NewBuffer(reqData))
	if err != nil {
		fmt.Fprintf(w, "ERROR")
		return // TODO
	}

	err = l.handleRequest(req)
	if err != nil {
		fmt.Fprintf(w, "ERROR")
		return // TODO
	}

	fmt.Fprintf(w, "OK")
}

func (l *Local) HandleDeregisterNode(w http.ResponseWriter, r *http.Request) {
	err := l.DeregisterNode()
	if err != nil {
		fmt.Fprintf(w, "ERROR")
		return // TODO
	}

	fmt.Fprintf(w, "OK")
}

func (l *Local) HandleDeregisterServices(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	servicesString := vars["services"]
	services := strings.FieldsFunc(servicesString, func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
	err := l.DeregisterServices(services...)
	if err != nil {
		fmt.Fprintf(w, "ERROR")
		return // TODO
	}

	fmt.Fprintf(w, "OK")
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

func (l *Local) DeregisterNode() error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/endpoints/%s", l.controlPlaneRESTURI, l.nodeID), nil)
	if err != nil {
		return err
	}
	return l.handleRequest(req)
}

func (l *Local) DeregisterServices(services ...string) error {
	commaDelimited := strings.Join(services, ",")
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/endpoints/%s/%s", l.controlPlaneRESTURI, l.nodeID, url.PathEscape(commaDelimited)), nil)
	if err != nil {
		return err
	}
	return l.handleRequest(req)
}

func (l *Local) handleRequest(req *http.Request) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		l.logger.Errorf("Deregister returned bad status code: %d", resp.StatusCode)
		return ErrBadResponse
	}

	return nil
}
