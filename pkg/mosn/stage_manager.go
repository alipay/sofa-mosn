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

package mosn

import (
	"time"

	"github.com/urfave/cli"
	"mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/configmanager"
	"mosn.io/mosn/pkg/log"
)

var MOSND *Mosn
// Data contains objects used in stages
type Data struct {
	// ctx contains the start parameters
	ctx *cli.Context
	// config path represents the config file path
	// mosn create basic config from it and if auto config dump is setted
	// new config data will write into this path
	configPath string
	// mosn basic config, created after parameters parsed stage
	config *v2.MOSNConfig
	// mosn struct contains some basic structures.
	mosn *Mosn
}

// StageManager is a stage manager that controls startup stages running.
// We divide the startup into four stages:
// 1. The parameters parsed stage. In this stage, parse different parameters from cli.Context,
// and finally call config load to create a MOSNConfig.
// 2. The initialize stage. In this stage, do some init actions based on config, and finally create a MOSN object.
// 3. The pre-startup stage. In this stage, creates some basic instance from MOSN object.
// 4. The startup stage. In this stage, do some startup actions such as connections transfer for smooth upgrade and so on.
// The difference between pre-startup stage and startup stage is that startup stage has already accomplished the resources
// that used to startup mosn.
type StageManager struct {
	data           Data
	started        bool
	paramsStages   []func(*cli.Context)
	initStages     []func(*v2.MOSNConfig)
	prestartStages []func(*Mosn)
	startupStages  []func(*Mosn)
	newMosn        func(*v2.MOSNConfig) *Mosn // support unit-test
}

func NewStageManager(ctx *cli.Context, path string) *StageManager {
	return &StageManager{
		data: Data{
			configPath: path,
			ctx:        ctx,
		},
		started:        false,
		paramsStages:   []func(*cli.Context){},
		initStages:     []func(*v2.MOSNConfig){},
		prestartStages: []func(*Mosn){},
		startupStages:  []func(*Mosn){},
		newMosn:        NewMosn,
	}
}

func (stm *StageManager) AppendParamsParsedStage(f func(*cli.Context)) *StageManager {
	if f == nil || stm.started {
		log.StartLogger.Errorf("[stage] invalid stage function or mosn is already started")
		return stm
	}
	stm.paramsStages = append(stm.paramsStages, f)
	return stm
}

func (stm *StageManager) runParamsParsedStage() time.Duration {
	st := time.Now()
	for _, f := range stm.paramsStages {
		f(stm.data.ctx)
	}
	// after all registered stages are completed, call the last process: load config
	stm.data.config = configmanager.Load(stm.data.configPath)
	return time.Since(st)
}

func (stm *StageManager) AppendInitStage(f func(*v2.MOSNConfig)) *StageManager {
	if f == nil || stm.started {
		log.StartLogger.Errorf("[stage] invalid stage function or mosn is already started")
		return stm
	}
	stm.initStages = append(stm.initStages, f)
	return stm
}

func (stm *StageManager) runInitStage() time.Duration {
	st := time.Now()
	for _, f := range stm.initStages {
		f(stm.data.config)
	}
	// after all registered stages are completed, call the last process: new mosn
	stm.data.mosn = stm.newMosn(stm.data.config)
	MOSND = stm.data.mosn
	return time.Since(st)
}

func (stm *StageManager) AppendPreStartStage(f func(*Mosn)) *StageManager {
	if f == nil || stm.started {
		log.StartLogger.Errorf("[stage] invalid stage function or mosn is already started")
		return stm
	}
	stm.prestartStages = append(stm.prestartStages, f)
	return stm
}

func (stm *StageManager) runPreStartStage() time.Duration {
	st := time.Now()
	for _, f := range stm.prestartStages {
		f(stm.data.mosn)
	}
	return time.Since(st)
}

func (stm *StageManager) AppendStartStage(f func(*Mosn)) *StageManager {
	if f == nil || stm.started {
		log.StartLogger.Errorf("[stage] invalid stage function or mosn is already started")
		return stm
	}
	stm.startupStages = append(stm.startupStages, f)
	return stm
}

func (stm *StageManager) runStartStage() time.Duration {
	st := time.Now()
	for _, f := range stm.startupStages {
		f(stm.data.mosn)
	}
	// start mosn after all start stages finished
	stm.data.mosn.Start()
	return time.Since(st)
}

// Run blocks until the mosn is closed
func (stm *StageManager) Run() {
	stm.started = true
	//
	log.StartLogger.Infof("mosn parameters parsed cost: %v", stm.runParamsParsedStage())
	log.StartLogger.Infof("mosn init cost: %v", stm.runInitStage())
	log.StartLogger.Infof("mosn prepare to start cost: %v", stm.runPreStartStage())
	log.StartLogger.Infof("mosn start cost: %v", stm.runStartStage())
}

// WaitFinish waits mosn start finished.
// if Run is not called, return directly
func (stm *StageManager) WaitFinish() {
	if !stm.started {
		return
	}
	stm.data.mosn.Wait()
}
