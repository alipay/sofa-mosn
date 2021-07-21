/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package network

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"mosn.io/api"
	"mosn.io/mosn/pkg/config/v2"
)

type mockEventListener struct {
	recvStatus bool
	stopChan   chan struct{}
}

func (e *mockEventListener) GetRecvStatus() bool {
	return e.recvStatus
}

func (e *mockEventListener) SetRecvStatus(state bool) {
	e.recvStatus = state
}

func (e *mockEventListener) OnAccept(rawc net.Conn, useOriginalDst bool, oriRemoteAddr net.Addr, c chan api.Connection, buf []byte, listeners []api.ConnectionEventListener) {
	e.recvStatus = true
	rawc.Close()
}

func (e *mockEventListener) OnNewConnection(ctx context.Context, conn api.Connection) {
}

func (e *mockEventListener) OnClose() {}

func (e *mockEventListener) PreStopHook(ctx context.Context) func() error {
	return nil
}

func testBase(t *testing.T, addr net.Addr) {
	cfg := &v2.Listener{
		ListenerConfig: v2.ListenerConfig{
			Name:       "test_listener",
			Network:    addr.Network(),
			BindToPort: true,
		},
		PerConnBufferLimitBytes: 1024,
		Addr: addr,
	}
	ln := NewListener(cfg)

	el := &mockEventListener{}
	ln.SetListenerCallbacks(el)
	go ln.Start(nil, false) // start
	time.Sleep(3 * time.Second)
	check := func(t *testing.T) bool {
		conn, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			t.Logf("dial error: %v", err)
			return false
		}
		defer conn.Close()
		return true
	}
	if !check(t) {
		t.Error("listener start check failed")
	}
	// duplicate start, will be ignored, return directly
	for i := 0; i < 10; i++ {
		ch := make(chan struct{})
		go func() {
			ln.Start(nil, false)
			close(ch)
		}()
		select {
		case <-ch:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("start not be ignored")
		}
	}
	// close listener
	if err := ln.Close(nil); err != nil {
		t.Errorf("Close listener failed, %v", err)
	}
	time.Sleep(3 * time.Second)
	if check(t) {
		t.Error("listener closed, but still can be dial success")
	}
	// start, but not restart, will be failed
	go ln.Start(nil, false)
	time.Sleep(time.Second)
	if check(t) {
		t.Error("listener start")
	}
	// restart
	go ln.Start(nil, true)
	time.Sleep(time.Second)
	if !check(t) {
		t.Error("listener restart check failed")
	}
	ln.Close(context.Background())
}

func TestListenerTCPStart(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:10101")
	testBase(t, addr)
}

func TestListenerUDSStart(t *testing.T) {
	addr, _ := net.ResolveUnixAddr("unix", "/tmp/test.sock")
	testBase(t, addr)
}

func TestUDSToFileListener(t *testing.T) {
	path := "/tmp/test1.sock"
	syscall.Unlink(path)
	l, _ := net.Listen("unix", path)
	f, _ := l.(*net.UnixListener).File()
	lc, err := net.FileListener(f)
	if err != nil {
		t.Errorf("convert to file listener failed, %v", err)
	}
	f1, _ := lc.(*net.UnixListener).File()
	assert.Equal(t, f.Name(), f1.Name())
	l.Close()
}
