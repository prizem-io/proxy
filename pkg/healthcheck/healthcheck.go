// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//go:generate protoc --gofast_out=plugins=grpc:. health.proto

package healthcheck

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/prizem-io/api/v1"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/prizem-io/proxy/discovery"
)

type HealthStatus int

const (
	HealthStatusUnknown = iota
	HealthStatusStarting
	HealthStatusOK
	HealthStatusUnhealthy
	HealthStatusUnavailable
	HealthStatusDraining
	HealthStatusTimeout
	HealthStatusMisconfiguration
)

var healthStatusString = map[HealthStatus]string{
	HealthStatusUnknown:          "unknown",
	HealthStatusStarting:         "starting",
	HealthStatusOK:               "ok",
	HealthStatusUnhealthy:        "healthy",
	HealthStatusUnavailable:      "unavailable",
	HealthStatusDraining:         "draining",
	HealthStatusTimeout:          "timeout",
	HealthStatusMisconfiguration: "misconfiguration",
}

func (h HealthStatus) String() string {
	if s, ok := healthStatusString[h]; ok {
		return s
	}
	return ""
}

var (
	ErrServiceUnknown = errors.New("Unknown service")
	ErrNoCheckConfig  = errors.New("Health check is not configured for this service")
	ErrHandlerUnknown = errors.New("Unknown health check handler")
	ErrNodeUnknown    = errors.New("Unknown node")
)

var DefaultHandlers = Handlers{
	{
		Name:    "http",
		Handler: CheckHTTP,
	},
}

type NamedHandler struct {
	Name    string
	Handler Handler
}

type Handlers []NamedHandler

type Handler func(healthCheck *api.HealthCheck, node *api.Node, endpoint *api.ServiceInstance) (HealthStatus, error)

type Checker struct {
	nodeID    uuid.UUID
	routes    *discovery.Routes
	endpoints *discovery.Endpoints
	handlers  map[string]Handler
}

func New(nodeID uuid.UUID, routes *discovery.Routes, endpoints *discovery.Endpoints, handlers Handlers) *Checker {
	h := make(map[string]Handler, len(handlers))
	for _, handler := range handlers {
		h[handler.Name] = handler.Handler
	}
	return &Checker{
		nodeID:    nodeID,
		routes:    routes,
		endpoints: endpoints,
		handlers:  h,
	}
}

func (c *Checker) HealthCheck(serviceName string) (HealthStatus, error) {
	service, ok := c.routes.GetService(serviceName)
	if !ok {
		return HealthStatusUnknown, errors.Wrapf(ErrServiceUnknown, "Unknown service %s", serviceName)
	}

	if service.HealthCheck == nil {
		return HealthStatusUnknown, errors.Wrapf(ErrNoCheckConfig, "No health check configuration for service %s", serviceName)
	}

	handler, ok := c.handlers[service.HealthCheck.CheckType]
	if !ok {
		return HealthStatusMisconfiguration, errors.Wrapf(ErrHandlerUnknown, "Unknown handler for checker %s", service.HealthCheck.CheckType)
	}

	nodeServices, err := c.endpoints.GetNodeServices(c.nodeID)
	if err != nil {
		return HealthStatusUnknown, errors.Wrapf(ErrNodeUnknown, "Unknown node %s", c.nodeID)
	}

	var endpoint api.ServiceInstance
	var found bool
	for _, ns := range nodeServices.Services {
		if ns.Name == service.Name {
			endpoint = ns
			found = true
			break
		}
	}

	if !found {
		return HealthStatusUnknown, errors.Wrapf(ErrHandlerUnknown, "Could not locate endpoint for %s", service.Name)
	}

	return handler(service.HealthCheck, nodeServices, &endpoint)
}

type httpCheckConfig struct {
	Path string `mapstructure:"path"`
}

func CheckHTTP(healthCheck *api.HealthCheck, node *api.Node, endpoint *api.ServiceInstance) (HealthStatus, error) {
	config := httpCheckConfig{
		Path: "/health",
	}
	err := mapstructure.Decode(healthCheck.CheckConfig, &config)
	if err != nil {
		return HealthStatusUnknown, errors.Wrap(err, "Could not decode health check config")
	}

	port, ok := endpoint.Ports.Find("HealthCheck", "HTTP/2", "HTTP/1")
	if !ok {
		return HealthStatusUnknown, errors.Wrap(ErrServiceUnknown, "Could not locate port for HTTP")
	}

	address := fmt.Sprintf("http://%s:%d%s", node.Address, port.Port, config.Path)
	rs, err := http.Get(address)
	if err != nil {
		return TranslateNetworkError(err)
	}
	defer rs.Body.Close()

	if rs.StatusCode == 503 {
		return HealthStatusUnavailable, nil
	}
	switch rs.StatusCode / 100 {
	case 5:
		return HealthStatusUnhealthy, nil
	case 2:
		return HealthStatusOK, nil
	default:
		return HealthStatusUnknown, nil
	}
}

func GheckGRPC(healthCheck *api.HealthCheck, node *api.Node, endpoint *api.ServiceInstance) (HealthStatus, error) {
	config := httpCheckConfig{
		Path: "/grpc.health.v1.Health/Check",
	}
	err := mapstructure.Decode(healthCheck.CheckConfig, &config)
	if err != nil {
		return HealthStatusUnknown, errors.Wrap(err, "Could not decode health check config")
	}

	port, ok := endpoint.Ports.Find("HealthCheck", "GRPC")
	if !ok {
		return HealthStatusUnknown, errors.Wrap(ErrServiceUnknown, "Could not locate port for HTTP")
	}

	address := fmt.Sprintf("s:%d%s", node.Address, port.Port, config.Path)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return TranslateNetworkError(err)
	}
	defer conn.Close()

	client := NewHealthClient(conn)
	response, err := client.Check(context.Background(), &HealthCheckRequest{
		Service: endpoint.Name,
	})
	if err != nil {
		sc := status.Code(err)
		switch sc {
		case codes.Unavailable:
			return HealthStatusUnavailable, nil
		default:
			return HealthStatusUnknown, nil
		}
	}

	switch response.Status {
	case HealthCheckResponse_NOT_SERVING:
		return HealthStatusUnhealthy, nil
	case HealthCheckResponse_SERVING:
		return HealthStatusOK, nil
	default:
		return HealthStatusUnknown, nil
	}
}

func TranslateNetworkError(err error) (HealthStatus, error) {
	if uerr, ok := err.(*url.Error); ok {
		err = uerr.Err
	}
	if noerr, ok := err.(*net.OpError); ok {
		err = noerr.Err
	}
	if scerr, ok := err.(*os.SyscallError); ok {
		if scerr.Err == syscall.ECONNREFUSED {
			return HealthStatusUnavailable, nil
		}
	}
	return HealthStatusUnknown, err
}
