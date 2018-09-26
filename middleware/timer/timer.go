// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package timer

import (
	"time"

	"github.com/prizem-io/h2/proxy"
	log "github.com/sirupsen/logrus"

	"github.com/prizem-io/proxy/director"
)

type (
	Timer struct{}

	State struct {
		begin time.Time
	}
)

func Load(input interface{}) (proxy.Middleware, error) {
	return New(), nil
}

func New() *Timer {
	return &Timer{}
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
		log.Infof(
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
		log.Infof(
			"%s/%s took %v",
			proxyInfo.Service.Name,
			proxyInfo.Operation.Name,
			time.Since(state.begin))
		state.begin = time.Time{}
	}
	return ctx.Next(data, endStream)
}
