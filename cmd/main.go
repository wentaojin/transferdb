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
package main

import (
	"context"
	"github.com/wentaojin/transferdb/signal"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/pkg/errors"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/logger"

	"github.com/wentaojin/transferdb/server"
	"go.uber.org/zap"
)

func main() {
	cfg := config.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("start meta failed. error is [%s], Use '--help' for help.", err)
	}

	// 初始化日志 logger
	logger.NewZapLogger(cfg)
	config.RecordAppVersion("transferdb", cfg)

	go func() {
		if err := http.ListenAndServe(cfg.AppConfig.PprofPort, nil); err != nil {
			zap.L().Fatal("listen and serve pprof failed", zap.Error(errors.Cause(err)))
		}
		os.Exit(0)
	}()

	// 信号量监听处理
	signal.SetupSignalHandler(func() {
		os.Exit(1)
	})

	// 程序运行
	ctx := context.Background()
	if err := server.Run(ctx, cfg); err != nil {
		zap.L().Fatal("server run failed", zap.Error(errors.Cause(err)))
	}
}
