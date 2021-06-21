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

package variable

import (
	"context"
	"errors"
	"strings"
	"sync"

	"mosn.io/mosn/pkg/buffer"
	mosnctx "mosn.io/mosn/pkg/context"
	"mosn.io/mosn/pkg/types"
)

func init() {
	buffer.RegisterBuffer(&ins)
}

var (
	// global scope
	mux              sync.RWMutex
	variables        = make(map[string]Variable, 32) // all built-in variable definitions
	prefixVariables  = make(map[string]Variable, 32) // all prefix getter definitions
	indexedVariables = make([]Variable, 0, 32)       // indexed variables

	// error message
	errVariableDuplicated   = "duplicate variable register, name: "
	errPrefixDuplicated     = "duplicate prefix variable register, prefix: "
	errUndefinedVariable    = "undefined variable, name: "
	errInvalidContext       = "invalid context"
	errNoVariablesInContext = "no variables found in context"
	errSupportIndexedOnly   = "this operation only support indexed variable"
	errGetterNotFound       = "getter function undefined, variable name: "
	errSetterNotFound       = "setter function undefined, variable name: "
)

// ResetVariableForTest is a test function for reset the variables.
// DONOT call it in any non-test functions
func ResetVariableForTest() {
	mux.Lock()
	defer mux.Unlock()

	variables = make(map[string]Variable, 32)
	prefixVariables = make(map[string]Variable, 32)
	indexedVariables = make([]Variable, 0, 32)
}

// AddVariable is used to check variable name exists. Typical usage is variables used in access logs.
func AddVariable(name string) (Variable, error) {
	mux.Lock()
	defer mux.Unlock()

	// find built-in variables
	if variable, ok := variables[name]; ok {
		return variable, nil
	}

	// check prefix variables
	for prefix, variable := range prefixVariables {
		if strings.HasPrefix(name, prefix) {
			return variable, nil

			// TODO: index fast-path solution
			//// make it into indexed variables
			//indexed := NewIndexedVariable(name, name, variable.Getter(), variable.Setter(), variable.Flags())
			//// register indexed one
			//if err := RegisterVariable(indexed); err != nil {
			//	return nil, err
			//}
			//return indexed, nil
		}
	}

	return nil, errors.New(errUndefinedVariable + name)
}

func RegisterVariable(variable Variable) error {
	mux.Lock()
	defer mux.Unlock()

	name := variable.Name()

	// check conflict
	if _, ok := variables[name]; ok {
		return errors.New(errVariableDuplicated + name)
	}

	// register
	variables[name] = variable

	// check index
	if indexer, ok := variable.(Indexer); ok {
		index := len(indexedVariables)
		indexer.SetIndex(uint32(index))

		indexedVariables = append(indexedVariables, variable)
	}
	return nil
}

func RegisterPrefixVariable(prefix string, variable Variable) error {
	mux.Lock()
	defer mux.Unlock()

	// check conflict
	if _, ok := prefixVariables[prefix]; ok {
		return errors.New(errPrefixDuplicated + prefix)
	}

	// register
	prefixVariables[prefix] = variable
	return nil
}

var ins = variableBufferCtx{}

type variableBufferCtx struct {
	buffer.TempBufferCtx
}

func (ctx variableBufferCtx) New() interface{} {
	return &variableBuffer{
		vars: make([]IndexedValue, len(indexedVariables)),
	}
}

func (ctx variableBufferCtx) Reset(i interface{}) {
	buf, _ := i.(*variableBuffer)
	for idx := range buf.vars {
		buf.vars[idx].data = ""
		buf.vars[idx].noCacheable = false
		buf.vars[idx].NotFound = false
		buf.vars[idx].Valid = false
	}
}

type variableBuffer struct {
	vars []IndexedValue
}

func NewVariableContext(ctx context.Context) context.Context {
	poolCtx := buffer.PoolContext(ctx)
	variableBuf := poolCtx.Find(&ins, nil).(*variableBuffer)

	return mosnctx.WithValue(ctx, types.ContextKeyVariables, variableBuf.vars)
}
