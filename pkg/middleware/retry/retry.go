// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package retry

import (
	"strconv"
	"time"

	"github.com/prizem-io/api/v1"
	"github.com/prizem-io/h2/proxy"

	"github.com/prizem-io/proxy/pkg/director"
	"github.com/prizem-io/proxy/pkg/log"
)

type (
	Retry struct {
		logger            log.Logger
		defaultClassifier ResponseClassifier
	}

	State struct {
		shctx    proxy.SHContext
		sdctx    proxy.SDContext
		_sendBuf [10]frame
		sendBuf  []frame

		rhctx    proxy.RHContext
		rdctx    proxy.RDContext
		_recvBuf [10]frame
		recvBuf  []frame

		retry          *api.Retry
		classifier     ResponseClassifier
		requestHeaders proxy.Headers
		attempts       int
		done           chan struct{}
		doRetry        bool
	}

	frame struct {
		headers *proxy.HeadersParams
		data    []byte
	}
)

func Load(logger log.Logger, classifier ResponseClassifier) proxy.MiddlewareLoader {
	return func(input interface{}) (proxy.Middleware, error) {
		return New(logger, classifier), nil
	}
}

func New(logger log.Logger, defaultClassifier ResponseClassifier) *Retry {
	return &Retry{
		logger:            logger,
		defaultClassifier: defaultClassifier,
	}
}

func (f *Retry) Name() string {
	return "Retry"
}

func (f *Retry) InitialState() interface{} {
	s := &State{
		done: make(chan struct{}, 1),
	}
	s.sendBuf = s._sendBuf[:0]
	s.recvBuf = s._recvBuf[:0]
	return s
}

func (f *Retry) SendHeaders(ctx *proxy.SHContext, params *proxy.HeadersParams, endStream bool) error {
	state := ctx.State().(*State)

	state.shctx = *ctx // Make a copy of the send headers context
	state.sendBuf = append(state.sendBuf, frame{headers: params})

	f.checkState(ctx.Info, state)

	if state.requestHeaders == nil {
		state.requestHeaders = params.Headers
	}

	if endStream {
		proxyInfo := ctx.Info.(*director.ProxyInfo)
		go f.waitForCallback(ctx.Stream, proxyInfo, state)
	}
	return ctx.Next(params, endStream)
}

func (f *Retry) SendData(ctx *proxy.SDContext, data []byte, endStream bool) error {
	state := ctx.State().(*State)

	state.sdctx = *ctx // Make a copy of the send data context
	state.sendBuf = append(state.sendBuf, frame{data: data})

	f.checkState(ctx.Info, state)

	if endStream {
		proxyInfo := ctx.Info.(*director.ProxyInfo)
		go f.waitForCallback(ctx.Stream, proxyInfo, state)
	}
	return ctx.Next(data, endStream)
}

func (f *Retry) waitForCallback(stream *proxy.Stream, proxyInfo *director.ProxyInfo, state *State) {
	var timeoutDuration time.Duration
	if proxyInfo.Operation.Timeout != nil {
		timeoutDuration = time.Duration(*proxyInfo.Operation.Timeout)
	} else if proxyInfo.Service.Timeout != nil {
		timeoutDuration = time.Duration(*proxyInfo.Service.Timeout)
	}
	if timeoutDuration == 0 {
		// Default timeout
		timeoutDuration = 30 * time.Second
	}

	for {
		timer := time.NewTimer(timeoutDuration)

		select {
		case <-state.done:
			timer.Stop()
		case <-timer.C:
			// Prevent processing received data for this stream
			stream.Upstream.CancelStream(stream)
			proxy.RespondWithError(stream, proxy.ErrGatewayTimeout, 504)
			return
		}

		if state.retry == nil || !state.doRetry {
			// Send buffered response
			for i, f := range state.recvBuf {
				endStream := i == len(state.recvBuf)-1
				if f.headers != nil {
					hc := state.rhctx // copy
					err := hc.Next(f.headers, endStream)
					if err != nil {
						stream.Upstream.CancelStream(stream)
						proxy.RespondWithError(stream, proxy.ErrInternalServerError, 500)
						return
					}
				} else if f.data != nil {
					dc := state.rdctx // copy
					err := dc.Next(f.data, endStream)
					if err != nil {
						// At this point the response is most likely corrupt
						stream.Upstream.CancelStream(stream)
						proxy.RespondWithError(stream, proxy.ErrInternalServerError, 500)
						return
					}
				}
			}

			close(state.done)
			return
		}

		timeoutDuration = time.Duration(state.retry.PerTryTimeout)
		state.attempts++

		if state.doRetry {
			state.doRetry = false
			state.recvBuf = state.recvBuf[:0]
			f.logger.Infof("Retry attempt # %d", state.attempts)
			// TODO: Select new upstream connection ???
			stream.Upstream.RetryStream(stream)

			// Send buffered request
			for i, f := range state.sendBuf {
				endStream := i == len(state.sendBuf)-1
				if f.headers != nil {
					hc := state.shctx // copy
					err := hc.Next(f.headers, endStream)
					if err != nil {
						stream.Upstream.CancelStream(stream)
						proxy.RespondWithError(stream, proxy.ErrInternalServerError, 500)
						return
					}
				} else if f.data != nil {
					dc := state.sdctx // copy
					err := dc.Next(f.data, endStream)
					if err != nil {
						stream.Upstream.CancelStream(stream)
						proxy.RespondWithError(stream, proxy.ErrInternalServerError, 500)
						return
					}
				}
			}
		} else {
			proxy.RespondWithError(stream, proxy.ErrGatewayTimeout, 500)
			close(state.done)
			return
		}
	}
}

func (f *Retry) ReceiveHeaders(ctx *proxy.RHContext, params *proxy.HeadersParams, endStream bool) error {
	state := ctx.State().(*State)

	if state.retry != nil {
		statusHeader := params.ByName(":status")
		if statusHeader != "" {
			status, err := strconv.Atoi(statusHeader)
			if err != nil {
				return err
			}

			if state.requestHeaders != nil {
				responseType := state.classifier(state.requestHeaders, status)

				if responseType == RetryableFailure && state.attempts < state.retry.Attempts {
					// Prevent processing received data for this stream
					ctx.Upstream.CancelStream(ctx.Stream)
					state.doRetry = true
					state.done <- struct{}{}
					return nil
				}
			}
		}
	}

	state.rhctx = *ctx // Make a copy of the receive headers context
	state.recvBuf = append(state.recvBuf, frame{headers: params})

	if endStream {
		state.done <- struct{}{}
	}

	// Next is called from waitForCallback
	return nil
}

func (f *Retry) ReceiveData(ctx *proxy.RDContext, data []byte, endStream bool) error {
	state := ctx.State().(*State)

	state.rdctx = *ctx // Make a copy of the receive data context
	state.recvBuf = append(state.recvBuf, frame{data: data})

	if endStream {
		state.done <- struct{}{}
	}

	// Next is called from waitForCallback
	return nil
}

func (f *Retry) checkState(info interface{}, state *State) {
	if state.retry == nil {
		proxyInfo := info.(*director.ProxyInfo)
		if proxyInfo.Operation.Retry != nil {
			state.retry = proxyInfo.Operation.Retry
		} else if proxyInfo.Service.Retry != nil {
			state.retry = proxyInfo.Service.Retry
		}
	}
	if state.classifier == nil {
		state.classifier = f.defaultClassifier
		if state.retry != nil {
			if state.retry.ResponseClassifier != "" {
				c, ok := ResponseClassifiers[state.retry.ResponseClassifier]
				if ok {
					state.classifier = c
				} else {
					f.logger.Warnf("unknown response classifier %q", state.retry.ResponseClassifier)
				}
			}
		}
	}
}
