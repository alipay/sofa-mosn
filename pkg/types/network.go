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

package types

import (
	"context"
	"errors"
	"net"
	"os"
	"time"

	"github.com/rcrowley/go-metrics"
	"mosn.io/api"
	"mosn.io/mosn/pkg/config/v2"
)

//
//   The bunch of interfaces are structure skeleton to build a high performance, extensible network framework.
//
//   In mosn, we have 4 layers to build a mesh, net/io layer is the fundamental layer to support upper level's functionality.
//	 -----------------------
//   |        PROXY          |
//    -----------------------
//   |       STREAMING       |
//    -----------------------
//   |        PROTOCOL       |
//    -----------------------
//   |         NET/IO        |
//    -----------------------
//
//   Core model in network layer are listener and connection. Listener listens specified port, waiting for new connections.
//   Both listener and connection have a extension mechanism, implemented as listener and filter chain, which are used to fill in customized logic.
//   Event listeners are used to subscribe important event of Listener and Connection. Method in listener will be called on event occur, but not effect the control flow.
//   Filters are called on event occurs, it also returns a status to effect control flow. Currently 2 states are used: Continue to let it go, Stop to stop the control flow.
//   Filter has a callback handler to interactive with core model. For example, ReadFilterCallbacks can be used to continue filter chain in connection, on which is in a stopped state.
//
//   Listener:
//   	- Event listener
// 			- ListenerEventListener
//      - Filter
// 			- ListenerFilter
//   Connection:
//		- Event listener
// 			- ConnectionEventListener
//		- Filter
// 			- ReadFilter
//			- WriteFilter
//
//   Below is the basic relation on listener and connection:
//    --------------------------------------------------
//   |                                      			|
//   | 	  EventListener       EventListener     		|
//   |        *|                   |*          		    |
//   |         |                   |       				|
//   |        1|     1      *      |1          			|
// 	 |	    Listener --------- Connection      			|
//   |        1|      [accept]     |1          			|
//	 |         |                   |-----------         |
//   |        *|                   |*          |*       |
//   |	 ListenerFilter       ReadFilter  WriteFilter   |
//   |                                                  |
//    --------------------------------------------------
//

// Listener is a wrapper of tcp listener
type Listener interface {
	// Return config which initialize this listener
	Config() *v2.Listener

	// Set listener config
	SetConfig(config *v2.Listener)

	// Name returns the listener's name
	Name() string

	// Addr returns the listener's network address.
	Addr() net.Addr

	// Start starts listener with context
	Start(lctx context.Context, restart bool)

	// Stop stops listener
	// Accepted connections and listening sockets will not be closed
	Stop() error

	// ListenerTag returns the listener's tag, whichi the listener should use for connection handler tracking.
	ListenerTag() uint64

	// Set listener tag
	SetListenerTag(tag uint64)

	// ListenerFile returns a copy a listener file
	ListenerFile() (*os.File, error)

	// PerConnBufferLimitBytes returns the limit bytes per connection
	PerConnBufferLimitBytes() uint32

	// Set limit bytes per connection
	SetPerConnBufferLimitBytes(limitBytes uint32)

	// Set if listener should use original dst
	SetUseOriginalDst(use bool)

	// Get if listener should use original dst
	UseOriginalDst() bool

	// SetListenerCallbacks set a listener event listener
	SetListenerCallbacks(cb ListenerEventListener)

	// GetListenerCallbacks set a listener event listener
	GetListenerCallbacks() ListenerEventListener

	// Close closes listener, not closing connections
	Close(lctx context.Context) error

	// IsBindToPort
	IsBindToPort() bool
}

// ListenerEventListener is a Callback invoked by a listener.
type ListenerEventListener interface {
	// OnAccept is called on new connection accepted
	OnAccept(rawc net.Conn, useOriginalDst bool, oriRemoteAddr net.Addr, c chan api.Connection, buf []byte, listeners []api.ConnectionEventListener)

	// OnNewConnection is called on new mosn connection created
	OnNewConnection(ctx context.Context, conn api.Connection)

	// OnClose is called on listener close
	OnClose()

	// PreStopHook is called on listener quit(but before closed)
	PreStopHook(ctx context.Context) func() error
}

type ListenerFilter interface {
	// OnAccept is called when a raw connection is accepted, but before a Connection is created.
	OnAccept(cb ListenerFilterCallbacks) api.FilterStatus
}

// ListenerFilterCallbacks is a callback handler called by listener filter to talk to listener
type ListenerFilterCallbacks interface {
	// Conn returns the Connection reference used in callback handler
	Conn() net.Conn

	ContinueFilterChain(ctx context.Context, success bool)

	// SetOriginalAddr sets the original ip and port
	SetOriginalAddr(ip string, port int)
}

// ListenerFilterManager manages the listener filter
// Note: unsupport now
type ListenerFilterManager interface {
	AddListenerFilter(lf *ListenerFilter)
}

// ConnectionStats is a group of connection metrics
type ConnectionStats struct {
	ReadTotal     metrics.Counter
	ReadBuffered  metrics.Gauge
	WriteTotal    metrics.Counter
	WriteBuffered metrics.Gauge
}

// ClientConnection is a wrapper of Connection
type ClientConnection interface {
	api.Connection

	// connect to server in a async way
	Connect() error
}

// Default connection arguments
const (
	DefaultConnReadTimeout  = 15 * time.Second
	DefaultConnWriteTimeout = 15 * time.Second
	DefaultConnTryTimeout   = 60 * time.Second
	DefaultIdleTimeout      = 90 * time.Second
	DefaultUDPIdleTimeout   = 5 * time.Second
	DefaultUDPReadTimeout   = 1 * time.Second
)

// ConnectionHandler contains the listeners for a mosn server
type ConnectionHandler interface {
	// AddOrUpdateListener
	// adds a listener into the ConnectionHandler or
	// update a listener
	AddOrUpdateListener(lc *v2.Listener) (ListenerEventListener, error)

	//StartListeners starts all listeners the ConnectionHandler has
	StartListeners(lctx context.Context)

	// FindListenerByAddress finds and returns a listener by the specified network address
	FindListenerByAddress(addr net.Addr) Listener

	// FindListenerByName finds and returns a listener by the listener name
	FindListenerByName(name string) Listener

	// RemoveListeners find and removes a listener by listener name.
	RemoveListeners(name string)

	// StopListener stops a listener  by listener name
	StopListener(lctx context.Context, name string, stop bool) error

	// StopListeners stops all listeners the ConnectionHandler has.
	// The close indicates whether the listening sockets will be closed.
	StopListeners(lctx context.Context, close bool) error

	// ListListenersFD reports all listeners' fd
	ListListenersFile(lctx context.Context) []*os.File

	// StopConnection Stop Connection
	StopConnection()
}

type FilterChainFactory interface {
	CreateNetworkFilterChain(conn api.Connection)

	CreateListenerFilterChain(listener ListenerFilterManager)
}

var (
	ErrConnectionHasClosed    = errors.New("connection has closed")
	ErrWriteTryLockTimeout    = errors.New("write trylock has timeout")
	ErrWriteBufferChanTimeout = errors.New("writeBufferChan has timeout")
)
