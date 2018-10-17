// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/gorilla/mux"
	mixer "github.com/istio/api/mixer/v1"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prizem-io/h2/proxy"
	"github.com/satori/go.uuid"
	jaegerconfig "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	control "github.com/prizem-io/proxy/pkg/control/grpc"
	"github.com/prizem-io/proxy/pkg/director"
	"github.com/prizem-io/proxy/pkg/discovery"
	internallog "github.com/prizem-io/proxy/pkg/log"
	"github.com/prizem-io/proxy/pkg/middleware/istio"
	opentracingmw "github.com/prizem-io/proxy/pkg/middleware/opentracing"
	"github.com/prizem-io/proxy/pkg/middleware/retry"
	"github.com/prizem-io/proxy/pkg/middleware/timer"
	tlsreloader "github.com/prizem-io/proxy/pkg/tls"
	tracing "github.com/prizem-io/proxy/pkg/tracing/opentracing"
)

func init() {
	grpclog.SetLogger(log.New(ioutil.Discard, "", 0))
}

func main() {
	zapLogger, _ := zap.NewDevelopment()
	defer zapLogger.Sync() // flushes buffer, if any
	sugar := zapLogger.Sugar()
	logger := internallog.New(sugar)
	proxy.SetLogger(sugar)

	nodeID := uuid.NewV4()

	ingressListenPort := readEnvInt("INGRESS_PORT", 50052)
	egressListenPort := readEnvInt("EGRESS_PORT", 50062)
	registerListenPort := readEnvInt("REGISTER_PORT", 6060)
	controlPlaneRESTURI := readEnvString("REST_API_URI", "http://localhost:8000")
	controlPlaneGRPCURI := readEnvString("GRPC_API_TARGET", "localhost:9000")
	istioMixerURI := readEnvString("MIXER_TARGET", "localhost:9091")
	ingressCertPath := readEnvString("INGRESS_CERT_PATH", "etc/backend.cert")
	ingressKeyPath := readEnvString("INGRESS_KEY_PATH", "etc/backend.key")

	flag.IntVar(&ingressListenPort, "ingressPort", ingressListenPort, "The ingress listening port")
	flag.IntVar(&egressListenPort, "egressPort", egressListenPort, "The egress listening port")
	flag.IntVar(&registerListenPort, "registerPort", registerListenPort, "The register listening port")
	flag.StringVar(&controlPlaneRESTURI, "controlPlaneRESTURI", controlPlaneRESTURI, "The control plane REST URI")
	flag.StringVar(&controlPlaneGRPCURI, "controlPlaneGRPCURI", controlPlaneGRPCURI, "The control plane gRPC URI")
	flag.StringVar(&istioMixerURI, "istioMixerURI", istioMixerURI, "The Istio Mixer URI")
	flag.StringVar(&ingressCertPath, "ingressCertPath", ingressCertPath, "The ingress certificate path")
	flag.StringVar(&ingressKeyPath, "ingressKeyPath", ingressKeyPath, "The ingress key path")
	flag.Parse()

	// Load TLS key pair

	keyPairReloader, err := tlsreloader.NewKeyPairReloader(logger, ingressCertPath, ingressKeyPath)
	if err != nil {
		logger.Fatalf("Could not load key pair: %v", err)
	}

	tlsConfig := tls.Config{
		NextProtos:               []string{"h2", "h2-14", "http/1.1"},
		PreferServerCipherSuites: true,
		GetCertificate:           keyPairReloader.GetCertificateFunc(),
		InsecureSkipVerify:       true,
	}

	// Route & Endpoint Discovery

	policies := map[string]proxy.MiddlewareLoader{}

	eb := backoff.NewExponentialBackOff()
	notify := func(err error, d time.Duration) {
		logger.Infof("Failed attempt: %v -> will retry in %s", err, d)
	}

	logger.Infof("Control Plane URIs:")
	logger.Infof("- REST %s", controlPlaneRESTURI)
	logger.Infof("- gRPC %s", controlPlaneGRPCURI)

	r := discovery.NewRoutes(logger, controlPlaneRESTURI, policies)
	e := discovery.NewEndpoints(logger, controlPlaneRESTURI)
	l := discovery.NewLocal(logger, nodeID, controlPlaneRESTURI, ingressListenPort)

	logger.Infof("Connecting to control plane...")
	controller := control.New(logger, uuid.NewV4().String(), controlPlaneGRPCURI, r, e)
	err = backoff.RetryNotify(controller.Connect, eb, notify)
	if err != nil {
		logger.Fatalf("Could not connect to control plane: %v", err)
	}

	eb.Reset()
	logger.Infof("Subscribing to routes...")
	err = backoff.RetryNotify(controller.SubscribeToRoutes, eb, notify)
	if err != nil {
		logger.Fatalf("Could not subscribe to routes: %v", err)
	}
	defer controller.UnsubscribeFromRoutes()

	eb.Reset()
	logger.Infof("Subscribing to endpoints...")
	err = backoff.RetryNotify(controller.SubscribeToEndpoints, eb, notify)
	if err != nil {
		logger.Fatalf("Could not subscribe to endpoints: %v", err)
	}
	defer controller.UnsubscribeFromEndpoints()

	//////

	cfg, err := jaegerconfig.FromEnv()
	cfg.Sampler.Type = "const"
	cfg.Sampler.Param = 1
	t := tracing.New(func(serviceName string) (opentracing.Tracer, io.Closer, error) {
		return cfg.New(serviceName)
	})
	defer t.Close()

	//////

	logger.Infof("Connecting to Istio Mixer...")
	var conn *grpc.ClientConn
	eb.Reset()
	err = backoff.RetryNotify(func() (err error) {
		conn, err = grpc.Dial(istioMixerURI, grpc.WithInsecure())
		return
	}, eb, notify)
	if err != nil {
		logger.Fatalf("did not connect: %v", err)
	}
	client := mixer.NewMixerClient(conn)
	reporter := istio.NewReporter(client, 10000, 5*time.Second)
	outlierMonitor := director.NewOutlierMonitor(logger, 1000, 5, 30*time.Second)

	//////

	var g run.Group

	// Ingress listener (TLS) - connects to local services
	{
		var listener net.Listener
		g.Add(func() error {
			upstreams := director.NewUpstreams(20)
			d := director.New(logger, r.GetPathInfo, director.AlwaysService, e.GetSourceInstance, l.GetServiceNodes, upstreams, proxy.DefaultUpstreamDialers, nil, director.RoundRobin,
				timer.New(logger),
				istio.New(nodeID.String(), reporter.C, istio.Inbound),
				opentracingmw.New(logger, t, opentracingmw.Server),
			)

			logger.Infof("Proxy ingress starting on :%d", ingressListenPort)
			ln, err := net.Listen("tcp", fmt.Sprintf(":%d", ingressListenPort))
			if err != nil {
				return err
			}

			listener = tls.NewListener(ln, &tlsConfig)

			return proxy.Listen(listener, d.Direct)
		}, func(error) {
			if listener != nil {
				listener.Close()
			}
		})
	}
	// Egress listener - connects to remote services
	{
		var listener net.Listener
		g.Add(func() error {
			var err error
			upstreams := director.NewUpstreams(20)
			d := director.New(logger, r.GetPathInfo, outlierMonitor.IsServiceable, l.GetSourceInstance, e.GetServiceNodes, upstreams, proxy.DefaultUpstreamDialers, &tlsConfig, director.LeastLoad,
				retry.New(logger, retry.RetryableRead5XX, retry.NewUpstream, outlierMonitor),
				istio.New(nodeID.String(), reporter.C, istio.Outbound),
				opentracingmw.New(logger, t, opentracingmw.Client),
			)

			logger.Infof("Proxy egress starting on :%d", egressListenPort)
			listener, err = net.Listen("tcp", fmt.Sprintf(":%d", egressListenPort))
			if err != nil {
				return err
			}

			return proxy.Listen(listener, d.Direct)
		}, func(error) {
			if listener != nil {
				listener.Close()
			}
		})
	}
	// Health checking
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			//hc := healthcheck.New(nodeID, r, e, healthcheck.DefaultHandlers)
			t := time.Tick(time.Second)
			for {
				select {
				case <-t:
				/*status, err := hc.HealthCheck("whoamI")
				if err != nil {
					log.Error(err)
				} else if status != healthcheck.HealthStatusOK {
					log.Errorf("whoamI = %s", status)
				}*/
				case <-ctx.Done():
					return nil
				}
			}
		}, func(error) {
			cancel()
		})
	}
	// Registration & Profiling endpoint
	{
		r := mux.NewRouter()
		r.HandleFunc("/register", l.HandleRegister).Methods("POST")
		r.HandleFunc("/register", l.HandleDeregisterNode).Methods("DELETE")
		r.HandleFunc("/register/{services}", l.HandleDeregisterServices).Methods("DELETE")
		r.HandleFunc("/info", l.HandleInfo).Methods("GET")

		logger.Infof("Register starting on :%d", registerListenPort)
		listener, _ := net.Listen("tcp", fmt.Sprintf(":%d", registerListenPort))
		g.Add(func() error {
			return http.Serve(listener, r)
		}, func(error) {
			listener.Close()
			l.DeregisterNode()
		})
	}
	// Istio telemetry reporter
	{
		g.Add(func() error {
			return reporter.Process()
		}, func(error) {
			reporter.Close()
		})
	}
	// Outlier monitoring
	{
		g.Add(func() error {
			outlierMonitor.Process()
			return nil
		}, func(error) {
			outlierMonitor.Close()
		})
	}
	// This function just sits and waits for ctrl-C.
	{
		cancelInterrupt := make(chan struct{})
		g.Add(func() error {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
			select {
			case sig := <-c:
				return fmt.Errorf("received signal %s", sig)
			case <-cancelInterrupt:
				return nil
			}
		}, func(error) {
			close(cancelInterrupt)
		})
	}

	//////

	logger.Info("Proxy started")
	logger.Infof("exit %v", g.Run())
}

func readEnvString(key string, defaultValue string) string {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	return v
}

func readEnvInt(key string, defaultValue int) int {
	if i, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return i
	}
	return defaultValue
}
