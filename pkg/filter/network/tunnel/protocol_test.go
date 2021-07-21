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
)

func TestWriteBuffer(t *testing.T) {
	c := &ConnectionInitInfo{
		ClusterName: "test_c1",
		Weight:      10,
		HostName:    "test100",
	}
	buffer, err := Encode(c)
	if err != nil {
		assert.Error(t, err, "write connection info failed")
		return
	}

	assert.NotEmpty(t, buffer.Bytes(), "buffer is empty")

	type Temp struct {
		name string
		id   int64
	}

	temp := &Temp{
		name: "aaa",
		id:   100,
	}

	_, err = Encode(temp)
	if err == nil {
		assert.Error(t, err, "expect to fail but succeed")
	}
}
func TestWriteAndRead(t *testing.T) {
	c := &ConnectionInitInfo{
		ClusterName: "test_c1",
		Weight:      10,
		HostName:    "test100",
	}
	buffer, err := Encode(c)
	if err != nil {
		assert.Error(t, err, "write connection info failed")
		return
	}

	res, err := DecodeFromBuffer(buffer)
	if err != nil {
		assert.Error(t, err, "decode connection info failed")
		return
	}
	assert.EqualValues(t, res, c, "different between writes and reads")
}
