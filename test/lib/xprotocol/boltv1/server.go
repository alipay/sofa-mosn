package boltv1

import (
	"context"
	"net"
	"sync"

	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/protocol/xprotocol"
	"mosn.io/mosn/pkg/protocol/xprotocol/bolt"
	mtypes "mosn.io/mosn/pkg/types"
	"mosn.io/mosn/test/lib"
	"mosn.io/mosn/test/lib/types"
	"mosn.io/pkg/buffer"
)

func init() {
	lib.RegisterCreateServer("bolt", NewBoltServer)
}

type MockBoltServer struct {
	mutex sync.Mutex
	stats *types.ServerStats
	mgr   *ConnectionManager
	// configs
	muxConfigs map[string]*ResponseConfig
	addr       string
	// running
	listener net.Listener
}

// TODO: make xprotocol server
func NewBoltServer(config interface{}) types.MockServer {
	c, err := NewBoltServerConfig(config)
	if err != nil {
		return nil
	}
	stats := types.NewServerStats()
	if len(c.Mux) == 0 { // use default config
		c.Mux = map[string]*ResponseConfig{
			".*": &ResponseConfig{
				Condition:     nil, //  always matched true
				CommonBuilder: DefaultSucessBuilder,
				ErrorBuilder:  DefaultErrorBuilder,
			},
		}
	}
	return &MockBoltServer{
		stats: stats,
		mgr: &ConnectionManager{
			connections: make(map[uint64]net.Conn),
			stats:       stats,
		},
		muxConfigs: c.Mux,
		addr:       c.Addr,
	}
}

func (s *MockBoltServer) Start() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.listener != nil {
		return
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.DefaultLogger.Fatalf("listen %s failed", s.addr)
	}
	s.listener = ln
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					continue
				}
				return
			}
			go s.serveConn(conn)
		}
	}()

}

func (s *MockBoltServer) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.listener.Close()
	s.listener = nil
	s.mgr.Clean()
}

func (s *MockBoltServer) Stats() types.ServerStatsReadOnly {
	return s.stats
}

// TODO: mux support more protocol to make x protocol server
func (s *MockBoltServer) mux(req *bolt.Request) (result *ResponseConfig) {
	if resp, ok := s.muxConfigs[".*"]; ok {
		result = resp // if .* exists, use it as default
	}
	v, ok := req.Get(mtypes.RPCRouteMatchKey)
	if !ok {
		return
	}
	resp, ok := s.muxConfigs[v]
	if !ok {
		return
	}
	return resp

}

// TODO: HandleRequest support more protocol to make x protocol server
func (s *MockBoltServer) handle(buf buffer.IoBuffer) *ResponseToWrite {
	ctx := context.Background()
	engine := xprotocol.GetProtocol(bolt.ProtocolName)
	cmd, err := engine.Decode(ctx, buf)
	if cmd == nil || err != nil {
		return nil
	}
	req, ok := cmd.(*bolt.Request)
	if !ok {
		return nil
	}
	// select mux
	respconfig := s.mux(req)
	resp, status := respconfig.HandleRequest(req, engine)
	if resp != nil {
		iobuf, err := engine.Encode(ctx, resp)
		if err != nil {
			log.DefaultLogger.Errorf("engine encode error: %v", err)
			return nil
		}
		return &ResponseToWrite{
			StatusCode: status,
			Body:       iobuf.Bytes(),
		}
	}
	return nil
}

func (s *MockBoltServer) serveConn(conn net.Conn) {
	id := s.mgr.Add(conn)
	defer func() {
		s.mgr.Delete(id)
	}()
	iobuf := buffer.NewIoBuffer(10240)
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Temporary() {
				continue
			}
			return
		}
		if n > 0 {
			iobuf.Write(buf[:n])
			for iobuf.Len() > 0 {
				resp := s.handle(iobuf)
				if resp == nil {
					break
				}
				s.stats.Records().RecordRequest()
				conn.Write(resp.Body)
				s.stats.Records().RecordResponse(resp.StatusCode)
			}
		}

	}
}

type ConnectionManager struct {
	mutex       sync.Mutex
	index       uint64
	connections map[uint64]net.Conn
	stats       *types.ServerStats
}

func (mgr *ConnectionManager) Add(conn net.Conn) uint64 {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	mgr.index++
	mgr.connections[mgr.index] = conn
	mgr.stats.ActiveConnection()
	return mgr.index
}

func (mgr *ConnectionManager) Delete(id uint64) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	delete(mgr.connections, id)
	mgr.stats.CloseConnection()
}

func (mgr *ConnectionManager) Clean() {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	for id, conn := range mgr.connections {
		conn.Close()
		delete(mgr.connections, id)
		mgr.stats.CloseConnection()
	}
}

type ResponseToWrite struct {
	StatusCode int16
	Body       []byte
}
