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
package service

import (
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.Logger

// 初始化日志 logger
func NewZapLogger(cfg *CfgFile) (err error) {
	Logger, _, err = log.InitLogger(&log.Config{
		Level: strings.ToLower(cfg.LogConfig.LogLevel),
		File: log.FileLogConfig{
			Filename:   cfg.LogConfig.LogFile,
			MaxSize:    cfg.LogConfig.MaxSize,
			MaxDays:    cfg.LogConfig.MaxDays,
			MaxBackups: cfg.LogConfig.MaxBackups,
		},
	}, zap.AddStacktrace(zapcore.FatalLevel))
	if err != nil {
		return err
	}
	return
}
