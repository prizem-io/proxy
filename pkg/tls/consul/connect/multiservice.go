package connect

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/watch"
)

// MultiService represents multiple Consul services that accepts and/or connects via Connect.
// This can represent a service that only is a server, only is a client, or
// both.
//
// TODO(banks): Agent implicit health checks based on knowing which certs are
// available should prevent clients being routed until the agent knows the
// service has been delivered valid certificates. Once built, document that here
// too.
type MultiService struct {
	sync.RWMutex

	// client is the Consul API client. It must be configured with an appropriate
	// Token that has `service:write` policy on the provided service. If an
	// insufficient token is provided, the Service will abort further attempts to
	// fetch certificates and print a loud error message. It will not Close() or
	// kill the process since that could lead to a crash loop in every service if
	// ACL token was revoked. All attempts to dial will error and any incoming
	// connections will fail to verify. It may be nil if the Service is being
	// configured from local files for development or testing.
	client *api.Client

	baseTLS *tls.Config

	roots      *x509.CertPool
	rootsWatch *watch.Plan

	servicesMu sync.RWMutex
	services   map[string]service

	logger *log.Logger
}

type service struct {
	leafWatch *watch.Plan
	tlsCfg    *multiTLSConfig
	verifier  verifierFunc
}

// NewMultiService creates and starts a Service. The caller must close the returned
// service to free resources and allow the program to exit normally. This is
// typically called in a signal handler.
//
// Caller must provide client which is already configured to speak to the local
// Consul agent, and with an ACL token that has `service:write` privileges for
// the service specified.
func NewMultiService(client *api.Client) (*MultiService, error) {
	return NewMultiServiceWithLogger(client,
		log.New(os.Stderr, "", log.LstdFlags))
}

// NewMultiServiceWithLogger starts the service with a specified log.Logger.
func NewMultiServiceWithLogger(client *api.Client,
	logger *log.Logger) (*MultiService, error) {
	s := &MultiService{
		client:   client,
		baseTLS:  defaultTLSConfig(),
		logger:   logger,
		services: make(map[string]service, 10),
	}

	// Set up root and leaf watches
	p, err := watch.Parse(map[string]interface{}{
		"type": "connect_roots",
	})
	if err != nil {
		return nil, err
	}
	s.rootsWatch = p
	s.rootsWatch.HybridHandler = s.rootsWatchHandler

	go s.rootsWatch.RunWithClientAndLogger(client, s.logger)

	return s, nil
}

func (s *MultiService) AddService(serviceName string) error {
	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()

	p, err := watch.Parse(map[string]interface{}{
		"type":    "connect_leaf",
		"service": serviceName,
	})
	if err != nil {
		return err
	}

	tlsCfg := newMultiTLSConfig(s)
	p.HybridHandler = s.leafWatchHandler(tlsCfg)
	s.services[serviceName] = service{
		leafWatch: p,
		tlsCfg:    tlsCfg,
		verifier:  newServerSideVerifier(s.client, serviceName),
	}

	go p.RunWithClientAndLogger(s.client, s.logger)

	return nil
}

// SetRoots sets new roots.
func (s *MultiService) SetRoots(roots *x509.CertPool) error {
	s.Lock()
	defer s.Unlock()
	s.roots = roots
	//cfg.notify()
	return nil
}

// Roots returns the current CA root CertPool.
func (s *MultiService) Roots() *x509.CertPool {
	s.RLock()
	defer s.RUnlock()
	return s.roots
}

// ServerTLSConfig returns a *tls.Config that allows any TCP listener to accept
// and authorize incoming Connect clients. It will return a single static config
// with hooks to dynamically load certificates, and perform Connect
// authorization during verification. Service implementations do not need to
// reload this to get new certificates.
//
// At any time it may be possible that the Service instance does not have access
// to usable certificates due to not being initially setup yet or a prolonged
// error during renewal. The listener will be able to accept connections again
// once connectivity is restored provided the client's Token is valid.
//
// To prevent routing traffic to the app instance while it's certificates are
// invalid or not populated yet you may use Ready in a health check endpoint
// and/or ReadyWait during startup before starting the TLS listener. The latter
// only prevents connections during initial bootstrap (including permission
// issues where certs can never be issued due to bad credentials) but won't
// handle the case that certificates expire and an error prevents timely
// renewal.
func (s *MultiService) ServerTLSConfig() *tls.Config {
	copy := s.baseTLS.Clone()
	copy.RootCAs = s.roots
	copy.ClientCAs = s.roots
	copy.GetCertificate = func(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
		//println("MultiService::GetCertificate")
		s.servicesMu.RLock()
		defer s.servicesMu.RUnlock()

		service, ok := s.services[chi.ServerName]
		if !ok {
			return nil, fmt.Errorf("tls: service %q is not registered", chi.ServerName)
		}
		leaf := service.tlsCfg.Leaf()
		if leaf == nil {
			return nil, errors.New("tls: no certificates configured")
		}
		return leaf, nil
	}
	copy.GetConfigForClient = func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
		//println("MultiService::GetConfigForClient")
		s.servicesMu.RLock()
		defer s.servicesMu.RUnlock()

		service, ok := s.services[chi.ServerName]
		if !ok {
			return nil, fmt.Errorf("tls: service %q is not registered", chi.ServerName)
		}

		return service.tlsCfg.Get(service.verifier), nil
	}
	return copy
}

// Close stops the service and frees resources.
func (s *MultiService) Close() error {
	if s.rootsWatch != nil {
		s.rootsWatch.Stop()
	}

	s.servicesMu.Lock()
	defer s.servicesMu.Unlock()

	for _, service := range s.services {
		service.leafWatch.Stop()
	}
	return nil
}

func (s *MultiService) rootsWatchHandler(blockParam watch.BlockingParamVal, raw interface{}) {
	if raw == nil {
		return
	}
	v, ok := raw.(*api.CARootList)
	if !ok || v == nil {
		s.logger.Println("[ERR] got invalid response from root watch")
		return
	}

	// Got new root certificates, update the tls.Configs.
	roots := x509.NewCertPool()
	for _, root := range v.Roots {
		roots.AppendCertsFromPEM([]byte(root.RootCertPEM))
	}

	s.SetRoots(roots)
}

func (s *MultiService) leafWatchHandler(tlsCfg *multiTLSConfig) func(blockParam watch.BlockingParamVal, raw interface{}) {
	return func(blockParam watch.BlockingParamVal, raw interface{}) {
		if raw == nil {
			return // ignore
		}
		v, ok := raw.(*api.LeafCert)
		if !ok || v == nil {
			s.logger.Println("[ERR] got invalid response from root watch")
			return
		}

		// Got new leaf, update the tls.Configs
		cert, err := tls.X509KeyPair([]byte(v.CertPEM), []byte(v.PrivateKeyPEM))
		if err != nil {
			s.logger.Printf("[ERR] failed to parse new leaf cert: %s", err)
			return
		}

		tlsCfg.SetLeaf(&cert)
	}
}

// Ready returns whether or not both roots and a leaf certificate are
// configured. If both are non-nil, they are assumed to be valid and usable.
func (s *MultiService) Ready(serviceName string) bool {
	if service, ok := s.services[serviceName]; ok {
		return service.tlsCfg.Ready()
	}
	return false
}

// ReadyWait returns a chan that is closed when the the Service becomes ready
// for use for the first time. Note that if the Service is ready when it is
// called it returns a nil chan. Ready means that it has root and leaf
// certificates configured which we assume are valid. The service may
// subsequently stop being "ready" if it's certificates expire or are revoked
// and an error prevents new ones being loaded but this method will not stop
// returning a nil chan in that case. It is only useful for initial startup. For
// ongoing health Ready() should be used.
func (s *MultiService) ReadyWait(serviceName string) <-chan struct{} {
	if service, ok := s.services[serviceName]; ok {
		return service.tlsCfg.ReadyWait()
	}
	return nil
}

// multiTLSConfig represents the state for returning a tls.Config that can
// have root and leaf certificates updated dynamically with all existing clients
// and servers automatically picking up the changes. It requires initialising
// with a valid base config from which all the non-certificate and verification
// params are used. The base config passed should not be modified externally as
// it is assumed to be serialised by the embedded mutex.
type multiTLSConfig struct {
	service *MultiService
	leaf    *tls.Certificate

	// readyCh is closed when the config first gets both leaf and roots set.
	// Watchers can wait on this via ReadyWait.
	readyCh chan struct{}
}

// newMultiTLSConfig returns a dynamicTLSConfig constructed from base.
// base.Certificates[0] is used as the initial leaf and base.RootCAs is used as
// the initial roots.
func newMultiTLSConfig(service *MultiService) *multiTLSConfig {
	cfg := &multiTLSConfig{
		service: service,
	}
	if len(service.baseTLS.Certificates) > 0 {
		cfg.leaf = &service.baseTLS.Certificates[0]
		// If this does error then future calls to Ready will fail
		// It is better to handle not-Ready rather than failing
		if err := parseLeafX509Cert(cfg.leaf); err != nil && service.logger != nil {
			service.logger.Printf("[ERR] Error parsing configured leaf certificate: %v", err)
		}
	}
	if !cfg.Ready() {
		cfg.readyCh = make(chan struct{})
	}
	return cfg
}

func (cfg *multiTLSConfig) Get(v verifierFunc) *tls.Config {
	cfg.service.RLock()
	defer cfg.service.RUnlock()
	copy := cfg.service.baseTLS.Clone()
	copy.RootCAs = cfg.service.roots
	copy.ClientCAs = cfg.service.roots
	if v != nil {
		copy.VerifyPeerCertificate = func(rawCerts [][]byte, chains [][]*x509.Certificate) error {
			return v(cfg.Get(nil), rawCerts)
		}
	}
	copy.GetCertificate = func(blah *tls.ClientHelloInfo) (*tls.Certificate, error) {
		//println("ServerName1 = " + blah.ServerName)
		leaf := cfg.Leaf()
		if leaf == nil {
			return nil, errors.New("tls: no certificates configured")
		}
		return leaf, nil
	}
	copy.GetClientCertificate = func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		leaf := cfg.Leaf()
		if leaf == nil {
			return nil, errors.New("tls: no certificates configured")
		}
		return leaf, nil
	}
	copy.GetConfigForClient = func(blah *tls.ClientHelloInfo) (*tls.Config, error) {
		//println("ServerName2 = " + blah.ServerName)
		return cfg.Get(v), nil
	}
	return copy
}

// SetLeaf sets a new leaf.
func (cfg *multiTLSConfig) SetLeaf(leaf *tls.Certificate) error {
	cfg.service.Lock()
	defer cfg.service.Unlock()
	if err := parseLeafX509Cert(leaf); err != nil {
		return err
	}
	cfg.leaf = leaf

	cfg.notify()
	return nil
}

// notify is called under lock during an update to check if we are now ready.
func (cfg *multiTLSConfig) notify() {
	if cfg.readyCh != nil && cfg.leaf != nil && cfg.service.roots != nil && cfg.leaf.Leaf != nil {
		close(cfg.readyCh)
		cfg.readyCh = nil
	}
}

func (cfg *multiTLSConfig) VerifyLeafWithRoots() error {
	cfg.service.RLock()
	defer cfg.service.RUnlock()

	if cfg.service.roots == nil {
		return fmt.Errorf("No roots are set")
	} else if cfg.leaf == nil {
		return fmt.Errorf("No leaf certificate is set")
	} else if cfg.leaf.Leaf == nil {
		return fmt.Errorf("Leaf certificate has not been parsed")
	}

	_, err := cfg.leaf.Leaf.Verify(x509.VerifyOptions{Roots: cfg.service.roots})
	return err
}

// Leaf returns the current Leaf certificate.
func (cfg *multiTLSConfig) Leaf() *tls.Certificate {
	cfg.service.RLock()
	defer cfg.service.RUnlock()
	return cfg.leaf
}

// Ready returns whether or not both roots and a leaf certificate are
// configured. If both are non-nil, they are assumed to be valid and usable.
func (cfg *multiTLSConfig) Ready() bool {
	// not locking because VerifyLeafWithRoots will do that
	return cfg.VerifyLeafWithRoots() == nil
}

// ReadyWait returns a chan that is closed when the the Service becomes ready
// for use for the first time. Note that if the Service is ready when it is
// called it returns a nil chan. Ready means that it has root and leaf
// certificates configured but not that the combination is valid nor that
// the current time is within the validity window of the certificate. The
// service may subsequently stop being "ready" if it's certificates expire
// or are revoked and an error prevents new ones from being loaded but this
// method will not stop returning a nil chan in that case. It is only useful
// for initial startup. For ongoing health Ready() should be used.
func (cfg *multiTLSConfig) ReadyWait() <-chan struct{} {
	return cfg.readyCh
}
