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

// UpstreamSelection is an enumeration to determine if a retry
// attempt should use the same upstream or select a new one.
type UpstreamSelection int

const (
	// SameUpstream indicates that the same upstream should be retried.
	SameUpstream UpstreamSelection = iota
	// NewUpstream indicates that the request should be retried on a different upstream.
	NewUpstream
)

type (
	Retry struct {
		logger            log.Logger
		defaultClassifier ResponseClassifier
		upstreamSelection UpstreamSelection
		responseReporter  ResponseReporter
	}

	ResponseReporter interface {
		ReportSuccess(proxyInfo *director.ProxyInfo)
		ReportFailure(proxyInfo *director.ProxyInfo)
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

func Load(logger log.Logger, classifier ResponseClassifier, upstreamSelection UpstreamSelection, responseReporter ResponseReporter) proxy.MiddlewareLoader {
	return func(input interface{}) (proxy.Middleware, error) {
		return New(logger, classifier, upstreamSelection, responseReporter), nil
	}
}

func New(logger log.Logger, defaultClassifier ResponseClassifier, upstreamSelection UpstreamSelection, responseReporter ResponseReporter) *Retry {
	return &Retry{
		logger:            logger,
		defaultClassifier: defaultClassifier,
		upstreamSelection: upstreamSelection,
		responseReporter:  responseReporter,
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
	stream := ctx.Stream
	proxyInfo := ctx.Info.(*director.ProxyInfo)

	state.shctx = *ctx // Make a copy of the send headers context
	state.sendBuf = append(state.sendBuf, frame{headers: params})

	f.checkState(proxyInfo, state)

	if state.requestHeaders == nil {
		state.requestHeaders = params.Headers
	}

	if endStream {
		go f.waitForCallback(stream, proxyInfo, state)
	}
	return ctx.Next(params, endStream)
}

func (f *Retry) SendData(ctx *proxy.SDContext, data []byte, endStream bool) error {
	state := ctx.State().(*State)
	proxyInfo := ctx.Info.(*director.ProxyInfo)

	state.sdctx = *ctx // Make a copy of the send data context
	state.sendBuf = append(state.sendBuf, frame{data: data})

	f.checkState(proxyInfo, state)

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
			if f.responseReporter != nil {
				f.responseReporter.ReportFailure(proxyInfo)
			}
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
			switch f.upstreamSelection {
			case SameUpstream:
				stream.Upstream.RetryStream(stream)
			case NewUpstream:
				ok := stream.Connection.DirectStream(stream, state.requestHeaders)
				if !ok {
					return
				}
			}

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

				// For circuit breaking / Outlier detection
				// Report success or failure
				if f.responseReporter != nil {
					proxyInfo := ctx.Info.(*director.ProxyInfo)
					switch responseType {
					case Success:
						f.responseReporter.ReportSuccess(proxyInfo)
					default:
						f.responseReporter.ReportFailure(proxyInfo)
					}
				}

				// Retry a retryable failure if we have not exhausted the max retry attempts.
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

func (f *Retry) checkState(proxyInfo *director.ProxyInfo, state *State) {
	if state.retry == nil {
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
				classifier, ok := ResponseClassifiers[state.retry.ResponseClassifier]
				if ok {
					state.classifier = classifier
				} else {
					f.logger.Warnf("unknown response classifier %q", state.retry.ResponseClassifier)
				}
			}
		}
	}
}
