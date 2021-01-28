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

package buffer

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"

	"mosn.io/api/types"

	mosnctx "mosn.io/pkg/context"
)

const maxBufferPool = 16

var (
	index int32
	bPool = bufferPoolArray[:]
	vPool = new(valuePool)

	bufferPoolArray [maxBufferPool]bufferPool
	nullBufferValue [maxBufferPool]interface{}
)

// TempBufferCtx is template for BufferPoolCtx
type TempBufferCtx struct {
	index int
}

func (t *TempBufferCtx) Index() int {
	return t.index
}

func (t *TempBufferCtx) New() interface{} {
	return nil
}

func (t *TempBufferCtx) Reset(x interface{}) {
}

// ifaceWords is interface internal representation.
type ifaceWords struct {
	typ  unsafe.Pointer
	data unsafe.Pointer
}

// setIdex sets index, poolCtx must embedded TempBufferCtx
func setIndex(poolCtx BufferPoolCtx, i int) {
	p := (*ifaceWords)(unsafe.Pointer(&poolCtx))
	temp := (*TempBufferCtx)(p.data)
	temp.index = i
}

func RegisterBuffer(poolCtx BufferPoolCtx) {
	// frist index is 1
	i := atomic.AddInt32(&index, 1)
	if i >= maxBufferPool {
		panic("bufferSize over full")
	}
	bPool[i].ctx = poolCtx
	setIndex(poolCtx, int(i))
}

// bufferPool is buffer pool
type bufferPool struct {
	ctx BufferPoolCtx
	sync.Pool
}

type valuePool struct {
	sync.Pool
}

// Take returns a buffer from buffer pool
func (p *bufferPool) take() (value interface{}) {
	value = p.Get()
	if value == nil {
		value = p.ctx.New()
	}
	return
}

// Give returns a buffer to buffer pool
func (p *bufferPool) give(value interface{}) {
	p.ctx.Reset(value)
	p.Put(value)
}

// BufferValue is buffer pool's Value
type BufferValue struct {
	value    [maxBufferPool]interface{}
	transmit [maxBufferPool]interface{}
}

// NewBufferPoolContext returns a context with BufferValue
func NewBufferPoolContext(ctx context.Context) context.Context {
	return mosnctx.WithValue(ctx, types.ContextKeyBufferPoolCtx, newBufferValue())
}

// TransmitBufferPoolContext copy a context
func TransmitBufferPoolContext(dst context.Context, src context.Context) {
	sValue := PoolContext(src)
	if sValue.value == nullBufferValue {
		return
	}
	dValue := PoolContext(dst)
	dValue.transmit = sValue.value
	sValue.value = nullBufferValue
}

// newBufferValue returns BufferValue
func newBufferValue() (value *BufferValue) {
	v := vPool.Get()
	if v == nil {
		value = new(BufferValue)
	} else {
		value = v.(*BufferValue)
	}
	return
}

// Find returns buffer from BufferValue
func (bv *BufferValue) Find(poolCtx BufferPoolCtx, x interface{}) interface{} {
	i := poolCtx.Index()
	if i <= 0 || i > int(index) {
		panic("buffer should call buffer.RegisterBuffer()")
	}
	if bv.value[i] != nil {
		return bv.value[i]
	}
	return bv.Take(poolCtx)
}

// Take returns buffer from buffer pools
func (bv *BufferValue) Take(poolCtx BufferPoolCtx) (value interface{}) {
	i := poolCtx.Index()
	value = bPool[i].take()
	bv.value[i] = value
	return
}

// Give returns buffer to buffer pools
func (bv *BufferValue) Give() {
	if index <= 0 {
		return
	}
	// first index is 1
	for i := 1; i <= int(index); i++ {
		value := bv.value[i]
		if value != nil {
			bPool[i].give(value)
		}
		value = bv.transmit[i]
		if value != nil {
			bPool[i].give(value)
		}
	}
	bv.value = nullBufferValue
	bv.transmit = nullBufferValue

	// Give BufferValue to Pool
	vPool.Put(bv)
}

// PoolContext returns BufferValue by context
func PoolContext(ctx context.Context) *BufferValue {
	if ctx != nil {
		if val := mosnctx.Get(ctx, types.ContextKeyBufferPoolCtx); val != nil {
			return val.(*BufferValue)
		}
	}
	return newBufferValue()
}
