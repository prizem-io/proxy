// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package opentracing

import (
	"strconv"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/prizem-io/h2/proxy"

	"github.com/prizem-io/proxy/pkg/director"
	"github.com/prizem-io/proxy/pkg/log"
	tracing "github.com/prizem-io/proxy/pkg/tracing/opentracing"
)

type TraceType int

const (
	Client TraceType = iota
	Server
)

type (
	Tracer struct {
		logger    log.Logger
		tracer    *tracing.ProcessTracers
		traceType TraceType
	}

	State struct {
		span opentracing.Span
	}
)

func Load(logger log.Logger, tracer *tracing.ProcessTracers, traceType TraceType) proxy.MiddlewareLoader {
	return func(input interface{}) (proxy.Middleware, error) {
		return New(logger, tracer, traceType), nil
	}
}

func New(logger log.Logger, tracer *tracing.ProcessTracers, traceType TraceType) *Tracer {
	return &Tracer{
		logger:    logger,
		tracer:    tracer,
		traceType: traceType,
	}
}

func (f *Tracer) Name() string {
	return "Tracer"
}

func (f *Tracer) InitialState() interface{} {
	return &State{}
}

func (f *Tracer) SendHeaders(ctx *proxy.SHContext, params *proxy.HeadersParams, endStream bool) error {
	var span opentracing.Span
	state := ctx.State().(*State)
	proxyInfo := ctx.Info.(*director.ProxyInfo)
	method := params.ByName(":method")
	path := params.ByName(":path")

	tracer, err := f.tracer.Tracer(proxyInfo.Service.Name)
	if err != nil {
		f.logger.Warnf("could not get tracer for %q: %v", proxyInfo.Operation.Name, err)
		return ctx.Next(params, endStream)
	}

	switch f.traceType {
	case Client:
		spanContext, err := tracer.Extract(
			opentracing.HTTPHeaders,
			HTTPHeadersCarrier{params},
		)
		if err != nil && err != opentracing.ErrSpanContextNotFound {
			f.logger.Error("err", err)
		}

		span = tracer.StartSpan(proxyInfo.Operation.Name, opentracing.ChildOf(spanContext))
		ext.SpanKindRPCClient.Set(span)
		ext.HTTPMethod.Set(span, method)
		ext.HTTPUrl.Set(span, path)

		err = tracer.Inject(
			span.Context(),
			opentracing.HTTPHeaders,
			HTTPHeadersCarrier{params})
		if err != nil {
			f.logger.Error("err", err)
		}
	case Server:
		spanContext, err := tracer.Extract(
			opentracing.HTTPHeaders,
			HTTPHeadersCarrier{params},
		)
		if err != nil && err != opentracing.ErrSpanContextNotFound {
			f.logger.Error("err", err)
		}

		span = tracer.StartSpan(proxyInfo.Operation.Name, ext.RPCServerOption(spanContext))
		ext.HTTPMethod.Set(span, method)
		ext.HTTPUrl.Set(span, path)

		err = tracer.Inject(
			span.Context(),
			opentracing.HTTPHeaders,
			HTTPHeadersCarrier{params})
		if err != nil {
			f.logger.Error("err", err)
		}
	}

	state.span = span

	return ctx.Next(params, endStream)
}

func (f *Tracer) ReceiveHeaders(ctx *proxy.RHContext, params *proxy.HeadersParams, endStream bool) error {
	state := ctx.State().(*State)
	if state.span == nil {
		return ctx.Next(params, endStream)
	}

	statusHeader := params.ByName(":status")
	if statusHeader != "" {
		status, err := strconv.Atoi(statusHeader)
		if err == nil {
			ext.HTTPStatusCode.Set(state.span, uint16(status))
		}
	}

	if endStream {
		state.span.Finish()
		state.span = nil
	}
	return ctx.Next(params, endStream)
}

func (f *Tracer) ReceiveData(ctx *proxy.RDContext, data []byte, endStream bool) error {
	if endStream {
		state := ctx.State().(*State)
		if state.span != nil {
			state.span.Finish()
			state.span = nil
		}
	}
	return ctx.Next(data, endStream)
}

type HTTPHeadersCarrier struct {
	params *proxy.HeadersParams
}

// Set conforms to the TextMapWriter interface.
func (c HTTPHeadersCarrier) Set(key, val string) {
	c.params.Set(key, val)
}

// ForeachKey conforms to the TextMapReader interface.
func (c HTTPHeadersCarrier) ForeachKey(handler func(key, val string) error) error {
	for _, header := range c.params.Headers {
		if err := handler(header.Name, header.Value); err != nil {
			return err
		}
	}
	return nil
}
