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
package zlog

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.uber.org/zap"
	gormLogger "gorm.io/gorm/logger"
)

type GormLogger struct {
	ZapLogger        *zap.Logger
	LogLevel         gormLogger.LogLevel
	SlowThreshold    time.Duration
	SkipCallerLookup bool
	Colorful         bool
}

func NewGormLogger(zapLogger *zap.Logger, slowThreshold time.Duration) GormLogger {
	return GormLogger{
		ZapLogger:        zapLogger,
		LogLevel:         gormLogger.Warn,
		SlowThreshold:    slowThreshold * time.Millisecond,
		SkipCallerLookup: false,
		Colorful:         false,
	}
}

func (l GormLogger) SetAsDefault() {
	gormLogger.Default = l
}

func (l GormLogger) LogMode(level gormLogger.LogLevel) gormLogger.Interface {
	return GormLogger{
		ZapLogger:        l.ZapLogger,
		SlowThreshold:    l.SlowThreshold,
		LogLevel:         level,
		SkipCallerLookup: l.SkipCallerLookup,
		Colorful:         l.Colorful,
	}
}

func (l GormLogger) Info(ctx context.Context, str string, args ...interface{}) {
	if l.LogLevel < gormLogger.Info {
		return
	}
	l.logger().Sugar().Debugf(str, args...)
}

func (l GormLogger) Warn(ctx context.Context, str string, args ...interface{}) {
	if l.LogLevel < gormLogger.Warn {
		return
	}
	l.logger().Sugar().Warnf(str, args...)
}

func (l GormLogger) Error(ctx context.Context, str string, args ...interface{}) {
	if l.LogLevel < gormLogger.Error {
		return
	}
	l.logger().Sugar().Errorf(str, args...)
}

func (l GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= 0 {
		return
	}
	elapsed := time.Since(begin)
	switch {
	case err != nil && l.LogLevel >= gormLogger.Error:
		sql, rows := fc()
		l.logger().Error("SlowThreshold", zap.Error(err), zap.Duration("elapsed", elapsed), zap.Int64("rows", rows), zap.String("sql", sql))
	case l.SlowThreshold != 0 && elapsed > l.SlowThreshold && l.LogLevel >= gormLogger.Warn:
		sql, rows := fc()
		l.logger().Warn("SlowThreshold", zap.Duration("elapsed", elapsed), zap.Int64("rows", rows), zap.String("sql", sql))
	case l.LogLevel >= gormLogger.Info:
		sql, rows := fc()
		l.logger().Debug("SlowThreshold", zap.Duration("elapsed", elapsed), zap.Int64("rows", rows), zap.String("sql", sql))
	}
}

// 阻止涉及某些包的日志内容输出
// 当前涉及 gorm 包不会输出到日志文件
var (
	gormPackage = filepath.Join("gorm")
)

func (l GormLogger) logger() *zap.Logger {
	for i := 2; i < 15; i++ {
		_, file, _, ok := runtime.Caller(i)
		switch {
		case !ok:
		case strings.HasSuffix(file, "_test.go"):
		case strings.Contains(file, gormPackage):
		default:
			return l.ZapLogger.WithOptions(zap.AddCallerSkip(i))
		}
	}
	return l.ZapLogger
}
