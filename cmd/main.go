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
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/WentaoJin/transferdb/pkg/signal"

	"github.com/WentaoJin/transferdb/pkg/config"
	"github.com/WentaoJin/transferdb/server"
	"github.com/WentaoJin/transferdb/zlog"

	"go.uber.org/zap"
)

var (
	conf = flag.String("config", "config.toml", "specify the configuration file, default is config.toml")
	mode = flag.String("mode", "", "specify the program running mode: [prepare reverse full all]")
)

func main() {
	flag.Parse()
	go func() {
		if err := http.ListenAndServe(":9696", nil); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}()
	// 读取配置文件
	cfg, err := config.ReadConfigFile(*conf)
	if err != nil {
		log.Fatalf("read config file [%s] failed: %v", *conf, err)
	}
	// 初始化日志 logger
	if err := zlog.NewZapLogger(cfg); err != nil {
		log.Fatalf("create global zap logger failed: %v", err)
	}

	// 信号量监听处理
	signal.SetupSignalHandler(func(b bool) {
		os.Exit(0)
	})

	// 程序运行
	if err := server.Run(cfg, *mode); err != nil {
		zlog.Logger.Fatal("server run failed", zap.String("error", err.Error()))
	}
}
