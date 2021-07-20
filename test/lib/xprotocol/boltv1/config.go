package boltv1

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"mosn.io/api"
	"mosn.io/pkg/buffer"

	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/protocol"
	"mosn.io/mosn/pkg/protocol/xprotocol/bolt"
)

type BoltServerConfig struct {
	Addr string                     `json:"address"`
	Mux  map[string]*ResponseConfig `json:"mux_config"`
}

func NewBoltServerConfig(config interface{}) (*BoltServerConfig, error) {
	b, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	cfg := &BoltServerConfig{}
	if err := json.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

type ResponseConfig struct {
	Condition     *Condition       `json:"condition"`
	CommonBuilder *ResponseBuilder `json:"common_builder"`
	ErrorBuilder  *ResponseBuilder `json:"error_buidler"`
}

func (c *ResponseConfig) HandleRequest(req *bolt.Request, engine api.XProtocol) (resp api.XRespFrame, status int16) {
	switch req.CmdCode {
	case bolt.CmdCodeHeartbeat:
		// heartbeat
		resp = engine.Reply(context.TODO(), req)
		status = int16(bolt.ResponseStatusSuccess) // heartbeat must be success
	case bolt.CmdCodeRpcRequest:
		if c == nil {
			resp, status = DefaultErrorBuilder.Build(req)
			return
		}
		if c.Condition.Match(req) {
			resp, status = c.CommonBuilder.Build(req)
		} else {
			resp, status = c.ErrorBuilder.Build(req)
		}
	default:
		log.DefaultLogger.Errorf("invalid cmd code: %d", req.CmdCode)
	}
	return
}

type Condition struct {
	// If a request contains the expected header, matched this condition.
	// A request must have the configured header key and value, and can have others headers
	// which will be ingored.
	ExpectedHeader map[string]string `json:"expected_header"`
	// If a request contains the unexpected header, matched failed.
	UnexpectedHeaderKey []string `json:"unexpected_header_key"`
	// TODO: Add more condition
}

func (cond *Condition) Match(req *bolt.Request) bool {
	if cond == nil {
		return true
	}
	for key, value := range cond.ExpectedHeader {
		if v, ok := req.Get(key); !ok || !strings.EqualFold(v, value) {
			return false
		}
	}
	for _, key := range cond.UnexpectedHeaderKey {
		if _, ok := req.Get(key); ok {
			return false
		}
	}
	return true
}

// ResponseBuilder builds a bolt response based on the bolt request
type ResponseBuilder struct {
	// The response status code
	StatusCode int16 `json:"status_code"`
	// The response header that not contains the protocol header part.
	Header map[string]string `json:"header"`
	// The repsonse body content
	Body json.RawMessage `json:"body"`
}

func (b *ResponseBuilder) Build(req *bolt.Request) (*bolt.Response, int16) {
	reqId := uint32(req.GetRequestId())
	cmd := bolt.NewRpcResponse(reqId, uint16(b.StatusCode), protocol.CommonHeader(b.Header), nil)
	if len(b.Body) > 0 {
		cmd.Content = buffer.NewIoBufferBytes(b.Body)
	}
	return cmd, b.StatusCode
}

var DefaultSucessBuilder = &ResponseBuilder{
	StatusCode: int16(bolt.ResponseStatusSuccess),
}

var DefaultErrorBuilder = &ResponseBuilder{
	StatusCode: int16(bolt.ResponseStatusError),
}

type BoltClientConfig struct {
	TargetAddr string         `json:"target_address"`
	MaxConn    uint32         `json:"max_connection"`
	Request    *RequestConfig `json:"request_config"`
	Verify     *VerifyConfig  `json:"verify_config"`
}

func NewBoltClientConfig(config interface{}) (*BoltClientConfig, error) {
	b, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	cfg := &BoltClientConfig{}
	if err := json.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// RequestConfig decides what request to send
type RequestConfig struct {
	Header  map[string]string `json:"header"`
	Body    json.RawMessage   `json:"body"`
	Timeout time.Duration     `json:"timeout"` // request timeout
}

func (c *RequestConfig) BuildRequest(id uint32) (api.HeaderMap, buffer.IoBuffer) {
	if c == nil {
		return buildRequest(id, map[string]string{
			"service": "mosn-test-default-service", // must have service
		}, nil, -1)
	}
	return buildRequest(id, c.Header, c.Body, c.Timeout)
}

func buildRequest(id uint32, header map[string]string, body []byte, timeout time.Duration) (api.HeaderMap, buffer.IoBuffer) {
	var buf buffer.IoBuffer
	if len(body) > 0 {
		buf = buffer.NewIoBufferBytes(body)
	}
	req := bolt.NewRpcRequest(id, protocol.CommonHeader(header), buf)
	if timeout > 0 {
		req.Timeout = int32(int64(timeout) / 1e6)
	}
	return req, req.Content
}

// VerifyConfig describes what response want
type VerifyConfig struct {
	ExpectedStatusCode int16
	// if ExepctedHeader is nil, means do not care about header
	// if ExepctedHeader is exists, means resposne header should contain all the ExpectedHeader
	// If response header contain keys not in ExpectedHeader, we ignore it.
	// TODO: support regex
	ExpectedHeader map[string]string
	// if ExpectedBody is nil, means do not care about body
	// TODO: support regex
	ExpectedBody []byte
	// if MaxExpectedRT is zero, means do not care about rt
	// if MaxExpectedRT is not zero, means response's rt should no more than it
	MaxExpectedRT time.Duration
	// if MinExpectedRT is zero means do not care about it
	// if MinExpectedRT is not zero, means response's rt should more than it
	MinExpectedRT time.Duration
}

func (c *VerifyConfig) Verify(resp *Response) bool {
	if c == nil {
		return true
	}
	if resp.GetResponseStatus() != c.ExpectedStatusCode {
		log.DefaultLogger.Errorf("status is not expected: %d, %d", resp.GetResponseStatus(), c.ExpectedStatusCode)
		return false
	}
	for k, v := range c.ExpectedHeader {
		if value, ok := resp.Header.Get(k); !ok || !strings.EqualFold(v, value) {
			log.DefaultLogger.Errorf("header key %s is not expected, got: %s, %t, value: %s", k, v, ok, value)
			return false
		}
	}
	// if ExpectedBody is not nil, but length is zero, means expected empty content
	if c.ExpectedBody != nil {
		if !bytes.Equal(c.ExpectedBody, resp.Content.Bytes()) {
			log.DefaultLogger.Errorf("body is not expected: %v, %v", string(c.ExpectedBody), string(resp.Content.Bytes()))
			return false
		}
	}
	if c.MaxExpectedRT > 0 {
		if resp.Cost > c.MaxExpectedRT {
			log.DefaultLogger.Errorf("rt is %s, max expected is %s", resp.Cost, c.MaxExpectedRT)
			return false
		}
	}
	if c.MinExpectedRT > 0 {
		if resp.Cost < c.MinExpectedRT {
			log.DefaultLogger.Errorf("rt is %s, min expected is %s", resp.Cost, c.MaxExpectedRT)
			return false
		}
	}
	return true
}
