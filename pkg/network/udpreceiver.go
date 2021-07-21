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
	"strings"
	"sync"

	"mosn.io/api"
	"mosn.io/mosn/pkg/log"
	"mosn.io/pkg/buffer"
)

const UdpPacketMaxSize = 64 * 1024

var (
	ProxyMap = sync.Map{}
)

func GetProxyMapKey(raddr, laddr string) string {
	var builder strings.Builder
	builder.WriteString(raddr)
	builder.WriteString(":")
	builder.WriteString(laddr)
	return builder.String()
}

func SetUDPProxyMap(key string, conn api.Connection) {
	ProxyMap.Store(key, conn)
}

func DelUDPProxyMap(key string) {
	ProxyMap.Delete(key)
}

func readMsgLoop(lctx context.Context, l *listener) {
	conn := l.packetConn.(*net.UDPConn)
	buf := buffer.GetIoBuffer(UdpPacketMaxSize)
	defer buffer.PutIoBuffer(buf)
	for {
		buf.Reset()
		n, rAddr, err := conn.ReadFromUDP(buf.Bytes()[:buf.Cap()])
		buf.Grow(n)

		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				log.DefaultLogger.Infof("[network] [udp] listener %s stop receiving packets by deadline", l.name)
				return
			}
			if ope, ok := err.(*net.OpError); ok {
				// not timeout error and not temporary, which means the error is non-recoverable
				if !(ope.Timeout() && ope.Temporary()) {
					log.DefaultLogger.Alertf("listener.readMsgLoop", "[network] [udp] listener %s occurs non-recoverable error, stop listening and reading:%s", l.name, err.Error())
					return
				}
			}
			log.DefaultLogger.Errorf("[network] [udp] recv from udp error: %v", err)
			continue
		}

		proxyKey := GetProxyMapKey(conn.LocalAddr().String(), rAddr.String())
		if dc, ok := ProxyMap.Load(proxyKey); !ok {
			fd, _ := conn.File()
			clientConn, _ := net.FilePacketConn(fd)

			l.cb.OnAccept(clientConn.(*net.UDPConn), l.useOriginalDst, rAddr, nil, buf.Bytes()[0:n], nil)
		} else {
			c := dc.(api.Connection)
			c.OnRead(buf)
		}
	}
}
