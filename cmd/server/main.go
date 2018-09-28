package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/istio/api/mixer/v1"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/prizem-io/h2/proxy"
	"github.com/satori/go.uuid"
	jaegerconfig "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	control "github.com/prizem-io/proxy/pkg/control/grpc"
	"github.com/prizem-io/proxy/pkg/director"
	"github.com/prizem-io/proxy/pkg/discovery"
	"github.com/prizem-io/proxy/pkg/log"
	"github.com/prizem-io/proxy/pkg/middleware/istio"
	opentracingmw "github.com/prizem-io/proxy/pkg/middleware/opentracing"
	"github.com/prizem-io/proxy/pkg/middleware/timer"
	tlsreloader "github.com/prizem-io/proxy/pkg/tls"
	tracing "github.com/prizem-io/proxy/pkg/tracing/opentracing"
)

func main() {
	zapLogger, _ := zap.NewProduction()
	defer zapLogger.Sync() // flushes buffer, if any
	sugar := zapLogger.Sugar()
	logger := log.New(sugar)
	proxy.SetLogger(sugar)

	nodeID, _ := uuid.FromString("24bbe1f7-3ac0-4489-9450-e62f262f818b")

	ingressListenPort := readEnv("INGRESS_PORT", 50052)
	egressListenPort := readEnv("EGRESS_PORT", 50062)
	registerListenPort := readEnv("REGISTER_PORT", 6060)
	var controlPlaneRESTURI string
	var controlPlaneGRPCURI string

	flag.IntVar(&ingressListenPort, "ingressPort", ingressListenPort, "The ingress listening port")
	flag.IntVar(&egressListenPort, "egressPort", egressListenPort, "The egress listening port")
	flag.IntVar(&registerListenPort, "registerPort", registerListenPort, "The register listening port")
	flag.StringVar(&controlPlaneRESTURI, "controlPlaneRESTURI", "http://localhost:8000", "The control plane REST URI")
	flag.StringVar(&controlPlaneGRPCURI, "controlPlaneGRPCURI", "localhost:9000", "The control plane gRPC URI")
	flag.Parse()

	// Load TLS key pair

	keyPairReloader, err := tlsreloader.NewKeyPairReloader(logger, "etc/backend.cert", "etc/backend.key")
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

	r := discovery.NewRoutes(logger, controlPlaneRESTURI, policies)
	e := discovery.NewEndpoints(logger, controlPlaneRESTURI)
	l := discovery.NewLocal()

	logger.Infof("Connecting to control plane...")
	controller := control.New(logger, uuid.NewV4().String(), controlPlaneGRPCURI, r, e)
	err = backoff.Retry(controller.Connect, eb)
	if err != nil {
		logger.Fatalf("Could not connect to control plane: %v", err)
	}

	eb.Reset()
	err = backoff.Retry(controller.SubscribeToRoutes, eb)
	if err != nil {
		logger.Fatalf("Could not subscribe to routes: %v", err)
	}
	defer controller.UnsubscribeFromRoutes()

	eb.Reset()
	err = backoff.Retry(controller.SubscribeToEndpoints, eb)
	if err != nil {
		logger.Fatalf("Could not subscribe to endpoints: %v", err)
	}
	defer controller.UnsubscribeFromEndpoints()

	//////

	cfg := &jaegerconfig.Configuration{
		Sampler: &jaegerconfig.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &jaegerconfig.ReporterConfig{
			LogSpans: true,
		},
	}
	t := tracing.New(func(serviceName string) (opentracing.Tracer, io.Closer, error) {
		return cfg.New(serviceName)
	})
	defer t.Close()

	//////

	logger.Infof("Connecting to Istio Mixer...")
	var conn *grpc.ClientConn
	eb.Reset()
	err = backoff.Retry(func() (err error) {
		conn, err = grpc.Dial("localhost:9091", grpc.WithInsecure())
		return
	}, eb)
	if err != nil {
		logger.Fatalf("did not connect: %v", err)
	}
	client := v1.NewMixerClient(conn)

	//////

	var g run.Group

	// Ingress listener (TLS) - connects to local services
	{
		var listener net.Listener
		g.Add(func() error {
			upstreams := director.NewUpstreams(20)
			d := director.New(logger, r.GetPathInfo, e.GetSourceInstance, l.GetServiceNodes, upstreams, proxy.DefaultUpstreamDialers, nil, director.RoundRobin,
				timer.New(logger),
				istio.New(nodeID.String(), client, istio.Inbound),
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
			d := director.New(logger, r.GetPathInfo, l.GetSourceInstance, e.GetServiceNodes, upstreams, proxy.DefaultUpstreamDialers, &tlsConfig, director.LeastLoad,
				istio.New(nodeID.String(), client, istio.Outbound),
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
		http.HandleFunc("/register", l.HandleRegister(nodeID, controlPlaneRESTURI, ingressListenPort))

		logger.Infof("Register starting on :%d", registerListenPort)
		listener, _ := net.Listen("tcp", fmt.Sprintf("localhost:%d", registerListenPort))
		g.Add(func() error {
			return http.Serve(listener, nil)
		}, func(error) {
			listener.Close()
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

	logger.Infof("exit %v", g.Run())
}

func readEnv(key string, defaultValue int) int {
	if i, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return i
	}
	return defaultValue
}
