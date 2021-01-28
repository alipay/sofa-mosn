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

	"mosn.io/pkg/variable"
)

// Deprecated: use mosn.io/pkg/variable/api.go:GetVariableValue instead
func GetVariableValue(ctx context.Context, name string) (string, error) {
	// 1. find built-in variables
	return variable.GetVariableValue(ctx, name)
}

// Deprecated: use mosn.io/pkg/variable/api.go:SetVariableValue instead
func SetVariableValue(ctx context.Context, name, value string) error {
	return variable.SetVariableValue(ctx, name, value)
}
