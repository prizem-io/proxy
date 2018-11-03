package tls

import (
	"crypto/tls"
	"net"

	api "github.com/prizem-io/api/v1"
)

type Service interface {
	ServerTLSConfig() *tls.Config
	Dial(service *api.Service, node *api.Node, address string) (net.Conn, error)
}
