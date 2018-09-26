// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package opentracing

import (
	"io"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
)

type (
	ProcessTracers struct {
		factory Factory

		tracersMu sync.RWMutex
		tracers   map[string]opentracing.Tracer
		closers   map[string]io.Closer
	}

	Factory func(serviceName string) (opentracing.Tracer, io.Closer, error)
)

func New(factory Factory) *ProcessTracers {
	return &ProcessTracers{
		factory: factory,
		tracers: make(map[string]opentracing.Tracer, 10),
		closers: make(map[string]io.Closer, 10),
	}
}

func (t *ProcessTracers) Tracer(serviceName string) (opentracing.Tracer, error) {
	t.tracersMu.RLock()
	tracer, ok := t.tracers[serviceName]
	t.tracersMu.RUnlock()

	if ok {
		return tracer, nil
	}

	t.tracersMu.Lock()
	defer t.tracersMu.Unlock()

	tracer, closer, err := t.factory(serviceName)
	if err != nil {
		return nil, err
	}

	t.tracers[serviceName] = tracer
	if closer != nil {
		t.closers[serviceName] = closer
	}
	return tracer, nil
}

func (t *ProcessTracers) Close() error {
	t.tracersMu.Lock()
	defer t.tracersMu.Unlock()

	var multi error

	for _, closer := range t.closers {
		err := closer.Close()
		if err != nil {
			multi = err
		}
	}

	return multi
}
