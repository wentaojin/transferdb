/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package prepare

import (
	"time"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"
)

// 初始化程序表结构 - only prepare 阶段
// 同步环境准备
func TransferDBEnvPrepare(engine *service.Engine) error {
	startTime := time.Now()
	service.Logger.Info("prepare tansferdb env start")
	if err := engine.InitMysqlEngineDB(); err != nil {
		return err
	}
	if err := engine.InitDefaultValueMap(); err != nil {
		return err
	}
	endTime := time.Now()
	service.Logger.Info("prepare tansferdb env finished",
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
