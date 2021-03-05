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

package healthcheck

import (
	"net/http"
	"time"

	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/types"
)

const TimeoutCfgKey = "timeout"
const defaultTimeout = uint32(30)

type HTTPDialSessionFactory struct{}

func (f *HTTPDialSessionFactory) NewSession(cfg map[string]interface{}, host types.Host) types.HealthCheckSession {
	var ret HTTPDialSession

	ret.timeout = defaultTimeout

	if v, ok := cfg[TimeoutCfgKey]; ok {
		if vv, ok := v.(uint32); ok {
			ret.timeout = vv
		}
	}

	ret.url = host.AddressString()

	return &ret
}

type HTTPDialSession struct {
	url     string
	timeout uint32
}

func (s *HTTPDialSession) CheckHealth() bool {
	// default dial timeout, maybe already timeout by checker
	client := http.Client{
		Timeout: time.Second * time.Duration(s.timeout),
	}
	resp, err := client.Get(s.url)
	if err != nil {
		if log.DefaultLogger.GetLogLevel() >= log.INFO {
			log.DefaultLogger.Infof("[upstream] [health check] [tcpdial session] dial tcp for host %s error: %v", s.url, err)
		}
		return false
	}

	return resp.StatusCode == http.StatusOK
}

func (s *HTTPDialSession) OnTimeout() {}
