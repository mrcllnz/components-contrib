// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cloudstate

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cloudstateio/go-support/cloudstate"
	"github.com/cloudstateio/go-support/cloudstate/crdt"
	"github.com/cloudstateio/go-support/cloudstate/protocol"
	"github.com/dapr/components-contrib/state"
	kvstore_pb "github.com/dapr/components-contrib/state/cloudstate/proto/kv_store"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/golang/protobuf/ptypes"
	any "github.com/golang/protobuf/ptypes/any"
	empty "github.com/golang/protobuf/ptypes/empty"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/grpc"
)

const (
	host                    = "host"
	serverPort              = "serverPort"
	defaultOperationTimeout = time.Second * 10
)

//nolint:gochecknoglobals
var doOnce sync.Once

type CRDT struct {
	connection *grpc.ClientConn
	metadata   *crdtMetadata
	json       jsoniter.API

	logger logger.Logger
}

type crdtMetadata struct {
	host       string
	serverPort int
}

func NewCRDT(logger logger.Logger) *CRDT {
	return &CRDT{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
}

// Init does metadata and connection parsing
func (c *CRDT) Init(metadata state.Metadata) error {
	m, err := c.parseMetadata(metadata)
	if err != nil {
		return err
	}
	c.metadata = m
	go func() {
		c.startServer() // TODO: capture this error
	}()
	return nil
}

func (c *CRDT) startServer() error {
	// TODO: Configure port and interface for the server by API, https://github.com/cloudstateio/go-support/issues/30
	//c.metadata.host
	//c.metadata.serverPort
	server, err := cloudstate.New(protocol.Config{
		ServiceName:    "cloudstate.KeyValueStore",
		ServiceVersion: "0.1.0",
	})
	// TODO: Allow ReportError to log to a user defined logger, https://github.com/cloudstateio/go-support/issues/31
	//c.logger.Errorf("error from Cloudstate: %s", in.GetMessage())
	if err != nil {
		return err
	}
	err = server.RegisterCRDT(
		&crdt.Entity{
			ServiceName: "cloudstate.KeyValueStore",
			EntityFunc: func(crdt.EntityId) interface{} {
				return &value{}
			},
			SetFunc:     setFunc,
			DefaultFunc: defaultFunc,
			CommandFunc: func(entity interface{}, ctx *crdt.CommandContext, name string, msg interface{}) (*any.Any, error) {
				return entity.(*value).handleCommand(ctx, name, msg)
			},
		},
		protocol.DescriptorConfig{
			Service: "kv_store.proto",
		},
	)
	if err != nil {
		return err
	}
	if err := server.Run(); err != nil {
		c.logger.Fatalf("failed to run: %v", err)
		return err
	}
	return nil
}

// Since Cloudstate runs as a sidecar, we're pushing the connection init to be lazily executed when a request comes in to
// Give Cloudstate ample time to start and form a cluster.
func (c *CRDT) createConnectionOnce() error {
	var connError error
	doOnce.Do(func() {
		conn, err := grpc.Dial(c.metadata.host, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			connError = fmt.Errorf("couldn't establish connection to Cloudstate: %s", err)
		} else {
			c.connection = conn
		}
	})
	return connError
}

func (c *CRDT) parseMetadata(metadata state.Metadata) (*crdtMetadata, error) {
	m := crdtMetadata{}
	if val, ok := metadata.Properties[host]; ok && val != "" {
		m.host = val
	} else {
		return nil, fmt.Errorf("host field required")
	}

	if val, ok := metadata.Properties[serverPort]; ok && val != "" {
		port, err := strconv.Atoi(val)
		if err != nil {
			return nil, err
		}
		m.serverPort = port
	} else {
		return nil, fmt.Errorf("serverPort field required")
	}
	return &m, nil
}

func (c *CRDT) getClient() kvstore_pb.KeyValueStoreClient {
	return kvstore_pb.NewKeyValueStoreClient(c.connection)
}

// Get retrieves state from Cloudstate with a key
func (c *CRDT) Get(req *state.GetRequest) (*state.GetResponse, error) {
	err := c.createConnectionOnce()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultOperationTimeout)
	defer cancel()

	client := c.getClient()
	resp, err := client.GetState(ctx, &kvstore_pb.GetStateEnvelope{
		Key: req.Key,
	})
	if err != nil {
		return nil, err
	}

	stateResp := &state.GetResponse{}
	if resp.Data != nil {
		stateResp.Data = resp.Data.Value
	}
	return stateResp, nil
}

// Delete performs a delete operation
func (c *CRDT) Delete(req *state.DeleteRequest) error {
	err := c.createConnectionOnce()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultOperationTimeout)
	defer cancel()

	client := c.getClient()
	_, err = client.DeleteState(ctx, &kvstore_pb.DeleteStateEnvelope{
		Key:  req.Key,
		Etag: req.ETag,
	})
	return err
}

// BulkDelete performs a bulk delete operation
func (c *CRDT) BulkDelete(req []state.DeleteRequest) error {
	err := c.createConnectionOnce()
	if err != nil {
		return err
	}

	for i := range req {
		err = c.Delete(&req[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Set saves state into Cloudstate
func (c *CRDT) Set(req *state.SetRequest) error {
	err := c.createConnectionOnce()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultOperationTimeout)
	defer cancel()

	var bt []byte
	b, ok := req.Value.([]byte)
	if ok {
		bt = b
	} else {
		bt, _ = c.json.Marshal(req.Value)
	}

	client := c.getClient()
	_, err = client.SaveState(ctx, &kvstore_pb.SaveStateEnvelope{
		Key:  req.Key,
		Etag: req.ETag,
		Value: &any.Any{
			Value: bt,
		},
	})
	return err
}

// BulkSet performs a bulks save operation
func (c *CRDT) BulkSet(req []state.SetRequest) error {
	err := c.createConnectionOnce()
	if err != nil {
		return err
	}

	for i := range req {
		err = c.Set(&req[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// value embeds the chosen CRDT type used to store data in Cloudstate
type value struct {
	*crdt.LWWRegister
}

func (v *value) handleCommand(ctx *crdt.CommandContext, name string, msg interface{}) (*any.Any, error) {
	switch name {
	case "GetState":
		switch msg.(type) {
		case *kvstore_pb.GetStateEnvelope:
			return ptypes.MarshalAny(&kvstore_pb.GetStateResponseEnvelope{
				Data: v.Value(),
			})
		}
	case "DeleteState":
		switch msg.(type) {
		case *kvstore_pb.DeleteStateEnvelope:
			v.Set(&any.Any{})
			return ptypes.MarshalAny(&empty.Empty{})
		}
	case "SaveState":
		switch m := msg.(type) {
		case *kvstore_pb.SaveStateEnvelope:
			v.Set(m.GetValue())
			return ptypes.MarshalAny(&empty.Empty{})
		}
	}
	ctx.Fail(fmt.Errorf("command not handled: %v", name))
	return nil, nil
}

func setFunc(ctx *crdt.Context, c crdt.CRDT) {
	switch t := c.(type) {
	case *crdt.LWWRegister:
		switch v := ctx.Instance.(type) {
		case *value:
			v.LWWRegister = t
		default:
			ctx.Fail(fmt.Errorf("supplied CRDT: %+v cannot bet set to: %+v", c, ctx.Instance))
		}
	default:
		ctx.Fail(fmt.Errorf("supplied CRDT: %+v cannot bet set to: %+v", c, ctx.Instance))
	}
}

func defaultFunc(*crdt.Context) crdt.CRDT {
	return crdt.NewLWWRegister(nil)
}
