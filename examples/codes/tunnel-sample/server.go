package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"mosn.io/api"
	"mosn.io/mosn/pkg/protocol/xprotocol"
	"mosn.io/mosn/pkg/protocol/xprotocol/bolt"
	"mosn.io/mosn/pkg/types"
	"mosn.io/pkg/buffer"
)

type Server struct {
	Listener     net.Listener
	protocolName types.ProtocolName
	protocol     api.XProtocol
}

func NewServer(addr string, proto types.ProtocolName) *Server {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	protocol := xprotocol.GetProtocol(proto)
	if protocol == nil {
		fmt.Println("unknown protocol:" + proto)
		return nil
	}

	return &Server{
		Listener:     ln,
		protocolName: proto,
		protocol:     protocol,
	}
}

func (s *Server) Run() {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				fmt.Printf("[Xprotocol RPC Server] Accept temporary error: %v\n", ne)
				continue
			}
			return //not temporary error, exit
		}
		fmt.Println("[Xprotocol RPC Server] get connection :", conn.RemoteAddr().String())
		go s.Serve(conn)
	}
}

func (s *Server) Serve(conn net.Conn) {
	iobuf := buffer.NewIoBuffer(102400)
	for {
		now := time.Now()
		conn.SetReadDeadline(now.Add(30 * time.Second))
		buf := make([]byte, 10*1024)
		bytesRead, err := conn.Read(buf)
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				fmt.Printf("[Xprotocol RPC Server] Connect read error: %v\n", err)
				continue
			}

		}
		if bytesRead > 0 {
			iobuf.Write(buf[:bytesRead])
			for iobuf.Len() > 1 {
				cmd, _ := s.protocol.Decode(nil, iobuf)
				if cmd == nil {
					break
				}

				// handle request
				resp, err := s.HandleRequest(conn, cmd)
				if err != nil {
					fmt.Printf("[Xprotocol RPC Server] handle request error: %v\n", err)
					return
				}
				respData, err := s.protocol.Encode(context.Background(), resp)
				if err != nil {
					fmt.Printf("[Xprotocol RPC Server] encode response error: %v\n", err)
					return
				}
				conn.Write(respData.Bytes())
			}
		}
	}
}

func (s *Server) HandleRequest(conn net.Conn, cmd interface{}) (api.XRespFrame, error) {
	switch s.protocolName {
	case bolt.ProtocolName:
		if req, ok := cmd.(*bolt.Request); ok {
			switch req.CmdCode {
			case bolt.CmdCodeHeartbeat:
				hbAck := s.protocol.Reply(context.TODO(), req)
				fmt.Printf("[Xprotocol RPC Server] reponse bolt heartbeat, connection: %s, requestId: %d\n", conn.RemoteAddr().String(), req.GetRequestId())
				return hbAck, nil
			case bolt.CmdCodeRpcRequest:
				resp := bolt.NewRpcResponse(req.RequestId, bolt.ResponseStatusSuccess, nil, nil)
				fmt.Printf("[Xprotocol RPC Server] reponse bolt request, connection: %s, requestId: %d\n", conn.RemoteAddr().String(), req.GetRequestId())
				return resp, nil
			}
		}
	}
	return nil, errors.New("unknown protocol:" + string(s.protocolName))
}

func main() {
	if server := NewServer("127.0.0.1:8080", bolt.ProtocolName); server != nil {
		server.Run()
	}
}
