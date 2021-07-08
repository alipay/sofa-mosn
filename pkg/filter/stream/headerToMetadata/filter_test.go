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

package headerToMetadata

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"mosn.io/mosn/pkg/mock"
	"mosn.io/mosn/pkg/network"
	"mosn.io/mosn/pkg/protocol"
)

func TestFilterConfig(t *testing.T) {
	testcases := []struct {
		conf map[string]interface{}
		err  error
	}{
		{
			conf: map[string]interface{}{
				"request_rules": []map[string]interface{}{
					{
						"header":            "a",
						"on_header_present": map[string]string{"key": "apk", "value": "apv"},
					},
				},
			},
			err: nil,
		},
		{
			conf: map[string]interface{}{
				"request_rules": []map[string]interface{}{
					{
						"header":            "a",
						"on_header_present": map[string]string{"key": "apk", "value": "apv"},
						"on_header_missing": map[string]string{"key": "amk", "value": "amv"},
					},
				},
			},
			err: ErrBothPresentAndMissing,
		},
		{
			conf: map[string]interface{}{
				"request_rules": []map[string]interface{}{
					{
						"header": "a",
						"remove": true,
					},
				},
			},
			err: ErrNeedPresentOrMissing,
		},
		{
			conf: map[string]interface{}{
				"request_rules": []map[string]interface{}{
					{
						"header":            "",
						"on_header_present": map[string]string{"key": "apk", "value": "apv"},
					},
				},
			},
			err: ErrEmptyHeader,
		},
	}

	for _, tc := range testcases {
		_, err := CreateFilterFactory(tc.conf)
		assert.Equal(t, tc.err, err)
	}
}

func TestFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ri := &network.RequestInfo{}
	handler := mock.NewMockStreamReceiverFilterHandler(ctrl)
	handler.EXPECT().RequestInfo().AnyTimes().Return(ri)

	factor := &FilterFactory{Rules: []Rule{
		{
			Header: "h1",
			OnPresent: &KVPair{
				Key:   "meta1",
				Value: "shouldNotBeThisValue",
			},
			Remove: true,
		},
		{
			Header: "h2",
			OnMissing: &KVPair{
				Key:   "meta2",
				Value: "v2",
			},
			Remove: false,
		},
	}}

	headers := protocol.CommonHeader{
		"h1": "v1",
		"h3": "v3",
	}

	filter := NewFilter(factor)
	filter.SetReceiveFilterHandler(handler)
	filter.OnReceive(context.Background(), headers, nil, nil)

	meta := ri.MetaData()
	assert.Equal(t, len(meta), 2)
	assert.Equal(t, meta["meta1"], "v1")
	assert.Equal(t, meta["meta2"], "v2")
	assert.Equal(t, len(headers), 1)

	_, ok := headers["h1"]
	assert.False(t, ok)
}
