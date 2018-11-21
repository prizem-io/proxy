package director

import (
	"crypto/tls"
	"net"
	"strconv"

	"github.com/pkg/errors"
	api "github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"
)

type UpstreamDialer func(source *api.ServiceInstance, service *api.Service, node *api.Node, port *api.Port, dialer proxy.Dialer) (proxy.Upstream, error)

// NamedUpstreamDialer associates a name with an upstream dialer.
type NamedUpstreamDialer struct {
	Name   string
	Dailer UpstreamDialer
}

// UpstreamDialers is a collection of UpstreamDialer that allows for lookup via `ForName`.
type UpstreamDialers []NamedUpstreamDialer

// ForName returns a `ConnectionFactory` and `ok` as true given a name if the connection factory is registered.
func (u UpstreamDialers) ForName(name string) (factory UpstreamDialer, ok bool) {
	// Using a slice assuming few enties would exist and therefore perform better than a map.
	for _, item := range u {
		if item.Name == name {
			return item.Dailer, true
		}
	}

	return nil, false
}

var DefaultUpstreamDialers = UpstreamDialers{
	{
		Name: "HTTP/2",
		Dailer: func(_ *api.ServiceInstance, service *api.Service, node *api.Node, port *api.Port, dialer proxy.Dialer) (proxy.Upstream, error) {
			address := net.JoinHostPort(node.Address.String(), strconv.Itoa(int(port.Port)))

			conn, err := dialer(address, port.Secure)
			if err != nil {
				return nil, errors.Wrapf(err, "could not connect to %s", address)
			}

			return proxy.NewH2Upstream(conn)
		},
	},
	{
		Name: "HTTP/1",
		Dailer: func(_ *api.ServiceInstance, service *api.Service, node *api.Node, port *api.Port, dialer proxy.Dialer) (proxy.Upstream, error) {
			address := net.JoinHostPort(node.Address.String(), strconv.Itoa(int(port.Port)))
			return proxy.NewH1Upstream(address, port.Secure, dialer)
		},
	},
}

var SimpleDailer = func(tlsConfig *tls.Config) proxy.Dialer {
	return func(address string, secure bool) (net.Conn, error) {
		if secure {
			return proxy.ConnectTLS(address, tlsConfig)
		}
		return proxy.Connect(address)
	}
}
