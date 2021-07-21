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

package tunnel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"mosn.io/mosn/pkg/filter/network/tunnel/ext"
)

func TestNewConnection(t *testing.T) {
	ext.RegisterConnectionCredentialGetter("test_credential", func(cluster string) string {
		return "aa"
	})

	config := ConnectionConfig{
		Address:           "127.0.0.1:9999",
		ClusterName:       "test",
		Weight:            100,
		ConnectRetryTimes: 3,
		Network:           "tcp",
		CredentialPolicy:  "test_credential",
	}
	got := NewAgentCoreConnection(config, nil)
	assert.Truef(t, got != nil, "create connection error")
	assert.Truef(t, got.initInfo.Credential == "aa", "credentialPolicy is incorrect")
}
