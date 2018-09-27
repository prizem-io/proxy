// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package istio

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/istio/api/mixer/v1"
	"github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"

	"github.com/prizem-io/proxy/pkg/director"
)

type TrafficDirection int

const (
	Inbound TrafficDirection = iota
	Outbound
)

type (
	Istio struct {
		nodeID    string
		client    v1.MixerClient
		direction TrafficDirection
	}

	State struct {
		requestTime             time.Time
		requestHasContentLength bool
		requestBodySize         int64
		requestHeaderSize       int64
		requestHeaders          map[string]string
		responseHeaders         map[string]string
		responseBodySize        int64
		responseHeaderSize      int64
		bag                     *MutableBag
	}
)

func Load(nodeID string, client v1.MixerClient, direction TrafficDirection) proxy.MiddlewareLoader {
	return func(input interface{}) (proxy.Middleware, error) {
		return New(nodeID, client, direction), nil
	}
}

func New(nodeID string, client v1.MixerClient, direction TrafficDirection) *Istio {
	return &Istio{
		nodeID:    nodeID,
		client:    client,
		direction: direction,
	}
}

func (f *Istio) Name() string {
	return "Istio"
}

func (f *Istio) InitialState() interface{} {
	return &State{
		bag: GetMutableBag(nil), //NewAttributes()
	}
}

func (f *Istio) SendHeaders(ctx *proxy.SHContext, params *proxy.HeadersParams, endStream bool) error {
	state := ctx.State().(*State)
	attrs := state.bag
	proxyInfo := ctx.Info.(*director.ProxyInfo)

	state.requestTime = time.Now()

	var sourceIP string
	var destAddr string

	switch f.direction {
	case Inbound:
		if ctx.Connection.RemoteAddr() != "" {
			sourceIP, _, _ = net.SplitHostPort(ctx.Connection.RemoteAddr())
		}
		destAddr = ctx.Connection.LocalAddr()
	case Outbound:
		if ctx.Connection.LocalAddr() != "" {
			sourceIP, _, _ = net.SplitHostPort(ctx.Connection.LocalAddr())
		}
		destAddr = ctx.Upstream.Address()
	}

	// SOURCE

	setString(attrs, "source.ip", sourceIP)

	si := proxyInfo.Source
	if si != nil {
		if f.direction == Outbound {
			params.Set("x-source-instance-id", si.ID.String())
		}
		attrs.Set("source.uid", si.ID.String())
		setString(attrs, "source.name", si.Name)
		setString(attrs, "source.namespace", si.Namespace)
		setString(attrs, "source.principal", si.Principal)
		setString(attrs, "source.owner", si.Owner)
		attrs.Set("source.labels", createMetadataMap(si.Metadata))
		attrs.Set("source.workload.uid", si.Service)
		attrs.Set("source.workload.name", si.Name)
		setString(attrs, "source.workload.namespace", si.Namespace)
	}

	// REQUEST

	setString(attrs, "request.id", params.ByName("x-request-id"))
	attrs.Set("request.path", params.ByName(":path"))
	attrs.Set("request.method", params.ByName(":method"))
	scheme := params.ByName(":scheme")
	attrs.Set("request.scheme", scheme)
	host := params.ByName(":authority")
	setString(attrs, "request.host", host)
	setString(attrs, "request.referer", params.ByName("referer"))
	contentLength := params.ByName("content-length")
	if contentLength != "" {
		attrs.Set("request.size", contentLength)
		state.requestHasContentLength = true
	}
	setString(attrs, "request.useragent", params.ByName("user-agent"))
	attrs.Set("request.time", state.requestTime)
	headers := state.requestHeaders
	if headers == nil {
		headers = make(map[string]string, len(params.Headers))
		state.requestHeaders = headers
	}
	for _, header := range params.Headers {
		if header.Name[0] != ':' {
			headers[header.Name] = header.Value
			state.requestHeaderSize += int64(len(header.Name) + len(header.Value) + 3)
		}
	}

	setString(attrs, "request.auth.principal", proxyInfo.Authorization.Principal)
	if len(proxyInfo.Authorization.Audiences) > 0 {
		attrs.Set("request.auth.audiences", proxyInfo.Authorization.Audiences)
	}
	setString(attrs, "request.auth.presenter", proxyInfo.Authorization.Presenter)
	if len(proxyInfo.Authorization.Claims) > 0 {
		attrs.Set("request.auth.claims", proxyInfo.Authorization.Claims)
	}
	setString(attrs, "request.api_key", proxyInfo.APIKey)

	// API

	attrs.Set("api.service", proxyInfo.Service.Name)
	attrs.Set("api.operation", proxyInfo.Operation.Name)
	attrs.Set("api.protocol", scheme)
	setString(attrs, "api.version", proxyInfo.Version)

	// CONTEXT

	switch f.direction {
	case Outbound:
		attrs.Set("context.reporter.kind", "outbound")
	case Inbound:
		attrs.Set("context.reporter.kind", "inbound")
	}

	attrs.Set("context.reporter.uid", f.nodeID)
	attrs.Set("context.protocol", "http")

	// Destination

	setIPPort(attrs, "destination.ip", "destination.port", destAddr)
	di := proxyInfo.Destination
	if di != nil {
		attrs.Set("destination.uid", di.ID.String())
		setString(attrs, "destination.name", di.Name)
		setString(attrs, "destination.namespace", di.Namespace)
		setString(attrs, "destination.principal", di.Principal)
		setString(attrs, "destination.owner", di.Owner)
		attrs.Set("destination.labels", createMetadataMap(di.Metadata))
		attrs.Set("destination.workload.uid", proxyInfo.Service.Name)
		attrs.Set("destination.workload.name", proxyInfo.Service.Name)
		setString(attrs, "destination.workload.namespace", proxyInfo.Service.Namespace)
		if di.Container != nil {
			setString(attrs, "destination.container.name", di.Container.Name)
			setString(attrs, "destination.container.image", di.Container.Image)
		}
	}

	setString(attrs, "destination.service.host", proxyInfo.Service.Hostname)
	attrs.Set("destination.service.uid", proxyInfo.Service.Name)
	attrs.Set("destination.service.name", proxyInfo.Service.Name)
	setString(attrs, "destination.service.namespace", proxyInfo.Service.Namespace)

	if endStream {
		endRequest(state)
	}

	return ctx.Next(params, endStream)
}

func (f *Istio) SendData(ctx *proxy.SDContext, data []byte, endStream bool) error {
	state := ctx.State().(*State)

	state.requestBodySize += int64(len(data))
	if endStream {
		endRequest(state)
	}

	return nil
}

func endRequest(state *State) {
	attrs := state.bag
	attrs.Set("request.headers", state.requestHeaders)
	if !state.requestHasContentLength {
		attrs.Set("request.size", state.requestBodySize)
	}
	attrs.Set("request.total_size", state.requestHeaderSize+state.requestBodySize)
}

func (f *Istio) ReceiveHeaders(ctx *proxy.RHContext, params *proxy.HeadersParams, endStream bool) error {
	state := ctx.State().(*State)
	attrs := state.bag

	statusStr := params.ByName(":status")
	if statusStr != "" {
		status, err := strconv.ParseInt(statusStr, 10, 64)
		if err == nil {
			attrs.Set("response.code", status)
		}
	}

	setString(attrs, "response.grpc_status", params.ByName("grpc-status"))
	setString(attrs, "response.grpc_message", params.ByName("grpc-message"))

	headers := state.responseHeaders
	if headers == nil {
		headers = make(map[string]string, len(params.Headers))
		state.responseHeaders = headers
	}
	for _, header := range params.Headers {
		if header.Name[0] != ':' {
			headers[header.Name] = header.Value
			state.responseHeaderSize += int64(len(header.Name) + len(header.Value) + 3)
		}
	}

	if endStream {
		f.endResponse(state)
	}
	return ctx.Next(params, endStream)
}

func (f *Istio) ReceiveData(ctx *proxy.RDContext, data []byte, endStream bool) error {
	state := ctx.State().(*State)
	state.responseBodySize += int64(len(data))
	if endStream {
		now := time.Now()
		attrs := state.bag
		attrs.Set("response.time", now)
		attrs.Set("response.size", state.responseBodySize)
		attrs.Set("response.duration", now.Sub(state.requestTime))
		f.sendReport(attrs)
	}
	return ctx.Next(data, endStream)
}

func (f *Istio) endResponse(state *State) {
	attrs := state.bag
	now := time.Now()
	attrs.Set("response.headers", state.responseHeaders)
	attrs.Set("response.time", now)
	attrs.Set("response.size", state.responseBodySize)
	attrs.Set("response.duration", now.Sub(state.requestTime))
	attrs.Set("response.total_size", state.responseHeaderSize+state.responseBodySize)
	f.sendReport(attrs)
}

func (f *Istio) sendReport(bag *MutableBag) {
	var attrs v1.CompressedAttributes
	bag.ToProto(&attrs, nil, 0)
	_, err := f.client.Report(context.Background(), &v1.ReportRequest{
		Attributes: []v1.CompressedAttributes{
			attrs,
		},
	})
	if err != nil {
		fmt.Println(err)
	}
}

func setString(attrs *MutableBag, key, value string) {
	if value != "" {
		attrs.Set(key, value)
	}
}

func setFirstString(attrs *MutableBag, key string, values ...string) {
	for _, value := range values {
		if value != "" {
			attrs.Set(key, value)
			return
		}
	}
}

func setIPPort(attrs *MutableBag, ipKey, portKey string, addr string) {
	if addr != "" {
		ip, port, err := net.SplitHostPort(addr)
		if err == nil {
			attrs.Set(ipKey, ip)
			if port != "" {
				if portInt, err := strconv.Atoi(port); err != nil {
					attrs.Set(portKey, portInt)
				}
			}
		}
	}
}

func createMetadataMap(metadata api.Metadata) map[string]string {
	labels := make(map[string]string, len(metadata))
	for key, val := range metadata {
		switch v := val.(type) {
		case string:
			labels[key] = v
		default:
			labels[key] = fmt.Sprintf("%v", v)
		}
	}
	return labels
}
