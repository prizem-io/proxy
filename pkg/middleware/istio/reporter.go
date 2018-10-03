// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package istio

import (
	"context"
	"time"

	mixer "github.com/istio/api/mixer/v1"
)

type Reporter struct {
	client   mixer.MixerClient
	duration time.Duration
	timer    *time.Timer

	C chan *MutableBag

	pos        int
	attributes []*MutableBag
	converted  []mixer.CompressedAttributes
	queue      [][]*MutableBag

	stop chan struct{}
	done chan struct{}
}

func NewReporter(client mixer.MixerClient, size int, duration time.Duration) *Reporter {
	return &Reporter{
		client:     client,
		duration:   duration,
		timer:      time.NewTimer(duration),
		C:          make(chan *MutableBag, size),
		attributes: make([]*MutableBag, size),
		converted:  make([]mixer.CompressedAttributes, size),
		queue:      make([][]*MutableBag, 0, 10),
		stop:       make(chan struct{}, 1),
		done:       make(chan struct{}, 1),
	}
}

func (r *Reporter) Process() error {
	defer func() { r.done <- struct{}{} }()

	for {
		var err error
		select {
		case <-r.timer.C:
			err = r.Flush()
		case a := <-r.C:
			r.attributes[r.pos] = a
			r.pos++
			if r.pos >= cap(r.attributes) {
				err = r.Flush()
			}
		case <-r.stop:
			err = r.Flush()
			return nil
		}

		if err != nil {
			// TODO
		}
	}
}

func (r *Reporter) Flush() error {
	r.timer.Reset(r.duration)

	if r.pos > 0 {
		r.queue = append(r.queue, r.attributes[:r.pos])
		r.attributes = make([]*MutableBag, cap(r.attributes))
		r.pos = 0
	}

	for len(r.queue) > 0 {
		q := r.queue[0]
		c := r.converted[:len(q)]
		for i := range q {
			c[i].Reset()
			q[i].ToProto(&c[i], nil, 0)
		}

		_, err := r.client.Report(context.Background(), &mixer.ReportRequest{
			Attributes: c,
		})
		if err != nil {
			return err
		}

		// Clean up
		for i := range q {
			q[i].Done()
		}
		r.queue = r.queue[:copy(r.queue[0:], r.queue[1:])]
	}

	return nil
}

func (r *Reporter) Stop() {
	close(r.stop)
	<-r.done
}
