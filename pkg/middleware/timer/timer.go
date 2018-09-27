// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package timer

import (
	"time"

	"github.com/prizem-io/h2/proxy"

	"github.com/prizem-io/proxy/pkg/director"
	"github.com/prizem-io/proxy/pkg/log"
)

type (
	Timer struct {
		logger log.Logger
	}

	State struct {
		begin time.Time
	}
)

func Load(logger log.Logger) proxy.MiddlewareLoader {
	return func(input interface{}) (proxy.Middleware, error) {
		return New(logger), nil
	}
}

func New(logger log.Logger) *Timer {
	return &Timer{
		logger: logger,
	}
}

func (f *Timer) Name() string {
	return "Timer"
}

func (f *Timer) InitialState() interface{} {
	return &State{}
}

func (f *Timer) SendHeaders(ctx *proxy.SHContext, params *proxy.HeadersParams, endStream bool) error {
	state := ctx.State().(*State)
	state.begin = time.Now()
	return ctx.Next(params, endStream)
}

func (f *Timer) SendData(ctx *proxy.SDContext, data []byte, endStream bool) error {
	state := ctx.State().(*State)
	if state.begin.IsZero() {
		state.begin = time.Now()
	}
	return ctx.Next(data, endStream)
}

func (f *Timer) ReceiveHeaders(ctx *proxy.RHContext, params *proxy.HeadersParams, endStream bool) error {
	if endStream {
		state := ctx.State().(*State)
		proxyInfo := ctx.Info.(*director.ProxyInfo)
		f.logger.Infof(
			"%s/%s took %v",
			proxyInfo.Service.Name,
			proxyInfo.Operation.Name,
			time.Since(state.begin))
		state.begin = time.Time{}
	}
	return ctx.Next(params, endStream)
}

func (f *Timer) ReceiveData(ctx *proxy.RDContext, data []byte, endStream bool) error {
	if endStream {
		state := ctx.State().(*State)
		proxyInfo := ctx.Info.(*director.ProxyInfo)
		f.logger.Infof(
			"%s/%s took %v",
			proxyInfo.Service.Name,
			proxyInfo.Operation.Name,
			time.Since(state.begin))
		state.begin = time.Time{}
	}
	return ctx.Next(data, endStream)
}
