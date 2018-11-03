package consul

import (
	"context"
	"crypto/tls"
	"io/ioutil"
	"log"
	"net"
	"sync"

	agentconnect "github.com/hashicorp/consul/agent/connect"
	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"

	proxyapi "github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"
	"github.com/prizem-io/proxy/pkg/discovery"
	proxytls "github.com/prizem-io/proxy/pkg/tls"
	"github.com/prizem-io/proxy/pkg/tls/consul/connect"
)

type Connect struct {
	client     *api.Client
	servicesMu sync.RWMutex
	services   map[string]*connect.Service
}

type Service struct {
	client  *api.Client
	service *connect.Service
}

func New(client *api.Client) *Connect {
	return &Connect{
		client:   client,
		services: make(map[string]*connect.Service),
	}
}

func (c *Connect) Close() error {
	var err error
	for _, service := range c.services {
		e := service.Close()
		if err == nil && e != nil {
			err = e
		}
	}
	return err
}

func (c *Connect) GetService(serviceName string) (proxytls.Service, error) {
	c.servicesMu.RLock()
	service, ok := c.services[serviceName]
	c.servicesMu.RUnlock()

	if !ok {
		var err error
		c.client.Connect()
		service, err = connect.NewServiceWithLogger(serviceName, c.client, log.New(ioutil.Discard, "", 0))
		if err != nil {
			return nil, errors.Wrapf(err, "could not create service %s", serviceName)
		}
		c.servicesMu.Lock()
		c.services[serviceName] = service
		c.servicesMu.Unlock()
	}

	return &Service{
		client:  c.client,
		service: service,
	}, nil
}

func (s *Service) ServerTLSConfig() *tls.Config {
	return s.service.ServerTLSConfig()
}

func (s *Service) Dial(service *proxyapi.Service, node *proxyapi.Node, address string) (net.Conn, error) {
	leafCert, _, err := s.client.Agent().ConnectCALeaf(service.Name, &api.QueryOptions{
		Datacenter: node.Datacenter,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "could not retrieve leaf cert for service %q", service.Name)
	}

	certURI, err := agentconnect.ParseCertURIFromString(leafCert.ServiceURI)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse cert URI: %s", leafCert.ServiceURI)
	}

	ctx := connect.ContextWithServerName(context.Background(), service.Name)

	return s.service.Dial(ctx, &connect.StaticResolver{
		Addr:    address,
		CertURI: certURI,
	})
}

func CheckConnection(conn net.Conn, pathInfo *discovery.PathInfo) error {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		state := tlsConn.ConnectionState()
		if state.ServerName != pathInfo.Service.Name {
			return errors.Wrapf(proxy.ErrNotFound, "could not call service: got %q wanted %q", state.ServerName, pathInfo.Service.Name)
		}
	}
	return nil
}
