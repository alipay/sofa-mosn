package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"

	"mosn.io/api"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/network"
	"mosn.io/mosn/pkg/protocol"
	"mosn.io/mosn/pkg/protocol/xprotocol/bolt"
	"mosn.io/mosn/pkg/stream"
	_ "mosn.io/mosn/pkg/stream/xprotocol"
	"mosn.io/mosn/pkg/types"
)

type Client struct {
	proto      types.ProtocolName
	Client     stream.Client
	conn       types.ClientConnection
	Id         uint64
	respWaiter sync.WaitGroup
	t          bool
}

func NewClient(addr string, proto types.ProtocolName, t bool) *Client {
	c := &Client{}
	stopChan := make(chan struct{})
	remoteAddr, _ := net.ResolveTCPAddr("tcp", addr)
	conn := network.NewClientConnection(0, nil, remoteAddr, stopChan)
	if err := conn.Connect(); err != nil {
		fmt.Println(err)
		return nil
	}
	// pass sub protocol to stream client
	ctx := context.WithValue(context.Background(), types.ContextSubProtocol, string(proto))
	c.Client = stream.NewStreamClient(ctx, protocol.Xprotocol, conn, nil)
	c.conn = conn
	c.proto = proto
	c.t = t
	return c
}

func (c *Client) OnReceive(ctx context.Context, headers types.HeaderMap, data types.IoBuffer, trailers types.HeaderMap) {
	fmt.Printf("[Xprotocol RPC Client] Receive Data:")
	if cmd, ok := headers.(api.XFrame); ok {
		streamID := protocol.StreamIDConv(cmd.GetRequestId())

		if resp, ok := cmd.(api.XRespFrame); ok {
			fmt.Println("stream:", streamID, " status:", resp.GetStatusCode())
			if !c.t {
				c.respWaiter.Done()
			}
		}
	}
}

func (c *Client) OnDecodeError(context context.Context, err error, headers types.HeaderMap) {}

func (c *Client) Request() {
	c.Id++
	requestEncoder := c.Client.NewStream(context.Background(), c)

	var request api.XFrame
	switch c.proto {
	case bolt.ProtocolName:
		request = bolt.NewRpcRequest(uint32(c.Id), protocol.CommonHeader(map[string]string{"service": "testSofa"}), nil)
	default:
		panic("unknown protocol, please complete the protocol-switch in Client.Request method")
	}

	requestEncoder.AppendHeaders(context.Background(), request.GetHeader(), true)
}

func main() {
	log.InitDefaultLogger("", log.DEBUG)
	t := flag.Bool("t", false, "-t")
	flag.Parse()
	// use bolt as example
	if client := NewClient("127.0.0.1:2045", bolt.ProtocolName, *t); client != nil {
		for {
			if !*t {
				client.respWaiter.Add(1)
			}
			client.Request()
			time.Sleep(200 * time.Millisecond)
			if !*t {
				client.respWaiter.Wait()
				return
			}
		}
	}
}
