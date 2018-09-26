// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package nats

import (
	"github.com/gogo/protobuf/proto"
	nats "github.com/nats-io/go-nats"
	"github.com/prizem-io/api/v1/convert"
	pb "github.com/prizem-io/api/v1/proto"
	log "github.com/sirupsen/logrus"

	"github.com/prizem-io/proxy/discovery"
)

type Connection struct {
	nodeID    string
	target    string
	routes    *discovery.Routes
	endpoints *discovery.Endpoints

	conn *nats.Conn
	rsub *nats.Subscription
	esub *nats.Subscription
}

func New(nodeID string, target string, routes *discovery.Routes, endpoints *discovery.Endpoints) *Connection {
	return &Connection{
		nodeID:    nodeID,
		target:    target,
		routes:    routes,
		endpoints: endpoints,
	}
}

func (c *Connection) Connect() error {
	var err error
	c.conn, err = nats.Connect(nats.DefaultURL)
	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) SubscribeToRoutes() error {
	var err error

	err = c.routes.RequestCatalog()
	if err != nil {
		return err
	}

	c.rsub, err = c.conn.Subscribe("routes", func(m *nats.Msg) {
		var msg pb.Message
		err := proto.Unmarshal(m.Data, &msg)
		if err != nil {
			log.Errorf("Error unmarshalling protobuf: %v", err)
			return
		}

		if msg.Type == "replicate" {
			if len(msg.Data) > 0 {
				var catalog pb.RoutesCatalog
				err := proto.Unmarshal(msg.Data, &catalog)
				if err != nil {
					log.Errorf("Error unmarshalling protobuf: %v", err)
					// TODO
				} else {
					if c.routes.StoreRoutes(catalog.Version, convert.DecodeServices(catalog.Services)) {
						log.Println("Stored from protobuf")
						return
					}
				}
			}

			err := c.routes.RequestCatalog()
			if err != nil {
				log.Error(err)
			}
			log.Println("Stored from rest")
		}
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) UnsubscribeFromRoutes() error {
	var err error
	if c.rsub != nil {
		err = c.rsub.Unsubscribe()
		c.rsub = nil
	}
	return err
}

func (c *Connection) SubscribeToEndpoints() error {
	var err error

	err = c.endpoints.RequestCatalog()
	if err != nil {
		return err
	}

	c.esub, err = c.conn.Subscribe("endpoints", func(m *nats.Msg) {
		var msg pb.Message
		err := proto.Unmarshal(m.Data, &msg)
		if err != nil {
			log.Errorf("Error unmarshalling protobuf: %v", err)
			return
		}

		if msg.Type == "replicate" {
			if len(msg.Data) > 0 {
				var catalog pb.EndpointsCatalog
				err := proto.Unmarshal(msg.Data, &catalog)
				if err != nil {
					log.Errorf("Error unmarshalling protobuf: %v", err)
					// TODO
				} else {
					if c.endpoints.StoreEndpoints(catalog.Version, convert.DecodeNodes(catalog.Nodes)) {
						log.Println("Stored from protobuf")
						return
					}
				}
			}

			err := c.endpoints.RequestCatalog()
			if err != nil {
				log.Error(err)
			}
			log.Println("Stored from rest")
		}
	})
	if err != nil {
		panic(err)
	}

	return nil
}

func (c *Connection) UnsubscribeFromEndpoints() error {
	var err error
	if c.esub != nil {
		err = c.esub.Unsubscribe()
		c.esub = nil
	}
	return err
}
