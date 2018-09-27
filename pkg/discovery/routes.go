// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package discovery

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"
	"github.com/prizem-io/routerstore"

	"github.com/prizem-io/proxy/pkg/log"
)

type (
	Routes struct {
		logger   log.Logger
		url      string
		policies map[string]proxy.MiddlewareLoader

		router  unsafe.Pointer
		catalog unsafe.Pointer

		mu      sync.RWMutex
		version int64
	}

	PathInfo struct {
		Service   *api.Service
		Operation *api.Operation
	}
)

var (
	ErrMiddlewareNotFound = errors.New("middleware not found")
)

func NewRoutes(logger log.Logger, baseURL string, policies map[string]proxy.MiddlewareLoader) *Routes {
	return &Routes{
		logger:   logger,
		url:      fmt.Sprintf("%s/v1/routes", baseURL),
		policies: policies,
	}
}

func (r *Routes) RequestCatalog() error {
	rs, err := http.Get(r.url)
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
			r.mu.RLock()
			ri = r.version
			r.mu.RUnlock()
			if version == ri {
				return nil
			}
		}
	}

	bodyBytes, err := ioutil.ReadAll(rs.Body)
	if err != nil {
		return err
	}

	var catalog api.RouteInfo
	err = json.Unmarshal(bodyBytes, &catalog)
	if err != nil {
		return err
	}

	r.StoreRoutes(version, catalog.Services)

	return nil
}

func (r *Routes) StoreRoutes(version int64, services []api.Service) bool {
	serviceMap := make(map[string]*api.Service)

	routes := routerstore.New()

	for i := range services {
		service := &services[i]
		serviceMap[service.Name] = service
		middleware, err := r.loadMiddleware(service.Policies)
		if err != nil {
			r.logger.Errorf("Policy error in service %s: %v", service.Name, err)
		}
		service.Middleware = middleware

		for i := range service.Operations {
			operation := &service.Operations[i]
			info := new(PathInfo)
			info.Service = service
			info.Operation = operation
			path := service.URIPrefix + operation.URIPattern
			routes.AddRoute(operation.Method, path, info)
			middleware, err := r.loadMiddleware(operation.Policies)
			if err != nil {
				r.logger.Errorf("Policy error in operation %s/%s: %v", service.Name, operation.Name, err)
			}
			operation.Middleware = middleware
		}
	}
	stored := false

	r.mu.Lock()
	if version != r.version {
		r.logger.Infof("Storing routes for updated index %d", version)
		atomic.StorePointer(&r.router, unsafe.Pointer(routes))
		atomic.StorePointer(&r.catalog, unsafe.Pointer(&serviceMap))
		r.version = version
		stored = true
	}
	r.mu.Unlock()

	return stored
}

func (r *Routes) RefreshLoop(duration time.Duration, stop chan struct{}) {
	t := time.Tick(duration)
	for {
		select {
		case <-t:
			err := r.RequestCatalog()
			if err != nil {
				r.logger.Errorf("Error requesting catalog: %v", err)
			}
		case <-stop:
			break
		}
	}
}

func (r *Routes) GetService(name string) (*api.Service, bool) {
	ptr := atomic.LoadPointer(&r.catalog)
	catalog := *(*map[string]*api.Service)(ptr)
	service, ok := catalog[name]
	return service, ok
}

func (r *Routes) GetPathInfo(method, path string) (*PathInfo, error) {
	ptr := atomic.LoadPointer(&r.router)
	routes := (*routerstore.RouteMux)(ptr)
	var result routerstore.Result
	err := routes.Match(method, path, &result)
	if err != nil {
		return nil, err
	}

	serviceDesc := result.Data.(*PathInfo)
	return serviceDesc, nil
}

func (r *Routes) loadMiddleware(policies []api.Configuration) ([]proxy.Middleware, error) {
	middlewares := make([]proxy.Middleware, len(policies))

	for i := range policies {
		loader, ok := r.policies[policies[i].Type]
		if !ok {
			return nil, errors.Wrap(ErrMiddlewareNotFound, policies[i].Type)
		}
		m, err := loader(policies[i].Config)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading middleware %s", policies[i].Type)
		}

		middlewares[i] = m
	}

	return middlewares, nil
}
