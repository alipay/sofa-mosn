package bolt

import (
	"context"
	"fmt"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/protocol/xprotocol"
	"mosn.io/mosn/pkg/types"
	"net/http"
)

/**
 * Request command protocol for v1
 * 0     1     2           4           6           8          10           12          14         16
 * +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
 * |proto| type| cmdcode   |ver2 |   requestID           |codec|        timeout        |  classLen |
 * +-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 * |headerLen  | contentLen            |                             ... ...                       |
 * +-----------+-----------+-----------+                                                                                               +
 * |               className + header  + content  bytes                                            |
 * +                                                                                               +
 * |                               ... ...                                                         |
 * +-----------------------------------------------------------------------------------------------+
 *
 * proto: code for protocol
 * type: request/response/request oneway
 * cmdcode: code for remoting command
 * ver2:version for remoting command
 * requestID: id of request
 * codec: code for codec
 * headerLen: length of header
 * contentLen: length of content
 *
 * Response command protocol for v1
 * 0     1     2     3     4           6           8          10           12          14         16
 * +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
 * |proto| type| cmdcode   |ver2 |   requestID           |codec|respstatus |  classLen |headerLen  |
 * +-----------+-----------+-----------+-----------+-----------+-----------+-----------+-----------+
 * | contentLen            |                  ... ...                                              |
 * +-----------------------+                                                                       +
 * |                         className + header  + content  bytes                                  |
 * +                                                                                               +
 * |                               ... ...                                                         |
 * +-----------------------------------------------------------------------------------------------+
 * respstatus: response status
 */

func init() {
	xprotocol.RegisterProtocol(ProtocolName, &boltProtocol{})
}

type boltProtocol struct{}

func (proto *boltProtocol) Name() types.ProtocolName {
	return ProtocolName
}

func (proto *boltProtocol) Encode(ctx context.Context, model interface{}) (types.IoBuffer, error) {
	switch frame := model.(type) {
	case *Request:
		return encodeRequest(ctx, frame)
	case *Response:
		return encodeResponse(ctx, frame)
	default:
		log.Proxy.Errorf(ctx, "[protocol][bolt] encode with unknown command : %+v", model)
		return nil, xprotocol.ErrUnknownType
	}
}

func (proto *boltProtocol) Decode(ctx context.Context, data types.IoBuffer) (interface{}, error) {
	if data.Len() >= LessLen {
		cmdType := data.Bytes()[1]

		switch cmdType {
		case CmdTypeRequest:
			return decodeRequest(ctx, data, false)
		case CmdTypeRequestOneway:
			return decodeRequest(ctx, data, true)
		case CmdTypeResponse:
			return decodeResponse(ctx, data)
		default:
			// unknown cmd type
			return nil, fmt.Errorf("Decode Error, type = %s, value = %d", UnKnownCmdType, cmdType)
		}
	}

	return nil, nil
}

// heartbeater
func (proto *boltProtocol) Trigger(requestId uint64) xprotocol.XFrame {
	return &Request{
		RequestHeader: RequestHeader{
			Protocol:  ProtocolCode,
			CmdType:   CmdTypeRequest,
			CmdCode:   CmdCodeHeartbeat,
			Version:   1,
			RequestId: uint32(requestId),
			Codec:     Hessian2Serialize, //todo: read default codec from config
			Timeout:   -1,
		},
	}
}

func (proto *boltProtocol) Reply(requestId uint64) xprotocol.XFrame {
	return &Response{
		ResponseHeader: ResponseHeader{
			Protocol:       ProtocolCode,
			CmdType:        CmdTypeResponse,
			CmdCode:        CmdCodeHeartbeat,
			Version:        ProtocolVersion,
			RequestId:      uint32(requestId),
			Codec:          Hessian2Serialize, //todo: read default codec from config
			ResponseStatus: ResponseStatusSuccess,
		},
	}
}

// hijacker
func (proto *boltProtocol) Hijack(statusCode uint32) xprotocol.XFrame {
	return &Response{
		ResponseHeader: ResponseHeader{
			Protocol:       ProtocolCode,
			CmdType:        CmdTypeResponse,
			CmdCode:        CmdCodeRpcResponse,
			Version:        ProtocolVersion,
			RequestId:      0,                 // this would be overwrite by stream layer
			Codec:          Hessian2Serialize, //todo: read default codec from config
			ResponseStatus: uint16(statusCode),
		},
	}
}

func (proto *boltProtocol) Mapping(httpStatusCode uint32) uint32 {
	switch httpStatusCode {
	case http.StatusOK:
		return uint32(ResponseStatusSuccess)
	case types.RouterUnavailableCode:
		return uint32(ResponseStatusNoProcessor)
	case types.NoHealthUpstreamCode:
		return uint32(ResponseStatusConnectionClosed)
	case types.UpstreamOverFlowCode:
		return uint32(ResponseStatusServerThreadpoolBusy)
	case types.CodecExceptionCode:
		//Decode or Encode Error
		return uint32(ResponseStatusCodecException)
	case types.DeserialExceptionCode:
		//Hessian Exception
		return uint32(ResponseStatusServerDeserialException)
	case types.TimeoutExceptionCode:
		//Response Timeout
		return uint32(ResponseStatusTimeout)
	default:
		return uint32(ResponseStatusUnknown)
	}
}