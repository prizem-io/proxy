// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package grpc

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/prizem-io/api/v1/convert"
	"github.com/prizem-io/api/v1/proto"
	"google.golang.org/grpc"

	"github.com/prizem-io/proxy/pkg/discovery"
	"github.com/prizem-io/proxy/pkg/log"
)

type Connection struct {
	logger    log.Logger
	nodeID    string
	target    string
	routes    *discovery.Routes
	endpoints *discovery.Endpoints

	conn    *grpc.ClientConn
	rstream proto.RouteDiscovery_StreamRoutesClient
	estream proto.EndpointDiscovery_StreamEndpointsClient
}

func New(logger log.Logger, nodeID string, target string, routes *discovery.Routes, endpoints *discovery.Endpoints) *Connection {
	return &Connection{
		logger:    logger,
		nodeID:    nodeID,
		target:    target,
		routes:    routes,
		endpoints: endpoints,
	}
}

func (c *Connection) Connect() error {
	var err error
	c.conn, err = grpc.Dial(c.target, grpc.WithInsecure())
	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) resubscribeToRoutes() {
	c.logger.Error("Lost connection with routes.  Attempting to reconnect...")
	go func() {
		for {
			time.Sleep(5 * time.Second)
			err := c.SubscribeToRoutes()
			if err == nil {
				c.logger.Info("Reconnected to routes")
				break
			}
		}
	}()
}

func (c *Connection) resubscribeToEndpoints() {
	c.logger.Error("Lost connection with endpoints.  Attempting to reconnect...")
	go func() {
		for {
			time.Sleep(5 * time.Second)
			err := c.SubscribeToEndpoints()
			if err == nil {
				c.logger.Info("Reconnected to endpoints")
				break
			}
		}
	}()
}

func (c *Connection) SubscribeToRoutes() error {
	var err error
	ctx := context.Background()
	routesClient := proto.NewRouteDiscoveryClient(c.conn)
	routesCatalog, err := routesClient.GetRoutes(ctx, &proto.RoutesRequest{})
	if err != nil {
		return errors.Wrap(err, "Could not discover routes")
	}
	c.routes.StoreRoutes(routesCatalog.Version, convert.DecodeServices(routesCatalog.Services))

	c.rstream, err = routesClient.StreamRoutes(ctx)
	if err != nil {
		return errors.Wrap(err, "Could not establish routes stream")
	}
	err = c.rstream.Send(&proto.RoutesRequest{
		NodeID:  c.nodeID,
		Version: routesCatalog.Version,
	})
	if err != nil {
		return errors.Wrap(err, "Error sending routes request")
	}
	go func(rstream proto.RouteDiscovery_StreamRoutesClient) {
		for {
			catalog, err := rstream.Recv()
			if err == io.EOF {
				c.logger.Error("EOF")
				return
			}
			if err != nil {
				c.logger.Errorf("Recv error: %v", err)
				c.resubscribeToRoutes()
				return
			}

			if !catalog.UseCache {
				c.routes.StoreRoutes(catalog.Version, convert.DecodeServices(catalog.Services))
			}
		}
	}(c.rstream)

	return nil
}

func (c *Connection) UnsubscribeFromRoutes() error {
	var err error
	if c.rstream != nil {
		err = c.rstream.CloseSend()
		c.rstream = nil
	}
	return err
}

func (c *Connection) SubscribeToEndpoints() error {
	var err error
	ctx := context.Background()
	endpointsClient := proto.NewEndpointDiscoveryClient(c.conn)
	endpointsCatalog, err := endpointsClient.GetEndpoints(ctx, &proto.EndpointsRequest{})
	if err != nil {
		return errors.Wrap(err, "Could not discover endpoints")
	}
	c.endpoints.StoreEndpoints(endpointsCatalog.Version, convert.DecodeNodes(endpointsCatalog.Nodes))
	c.estream, err = endpointsClient.StreamEndpoints(ctx)
	if err != nil {
		return errors.Wrap(err, "Could not establish endpoints stream")
	}
	err = c.estream.Send(&proto.EndpointsRequest{
		NodeID:  c.nodeID,
		Version: endpointsCatalog.Version,
	})
	if err != nil {
		return errors.Wrap(err, "Error sending endpoints request")
	}
	go func(estream proto.EndpointDiscovery_StreamEndpointsClient) {
		for {
			catalog, err := estream.Recv()
			if err == io.EOF {
				c.logger.Error("EOF")
				return
			}
			if err != nil {
				c.logger.Errorf("Recv error: %v", err)
				c.resubscribeToEndpoints()
				return
			}

			if !catalog.UseCache {
				c.endpoints.StoreEndpoints(catalog.Version, convert.DecodeNodes(catalog.Nodes))
			}
		}
	}(c.estream)

	return nil
}

func (c *Connection) UnsubscribeFromEndpoints() error {
	var err error
	if c.estream != nil {
		err = c.estream.CloseSend()
		c.estream = nil
	}
	return err
}
