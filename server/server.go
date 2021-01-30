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
package server

import (
	"fmt"
	"strings"

	"github.com/WentaoJin/transferdb/pkg/config"

	"github.com/WentaoJin/transferdb/pkg/reverser"

	"github.com/WentaoJin/transferdb/db"
)

func Run(cfg *config.CfgFile, mode string) error {
	// 程序运行
	switch strings.TrimSpace(mode) {
	case "prepare":
		// 初始化程序表结构 - only prepare 阶段
		mysqlEngine, err := db.NewMySQLEnginePrepareDB(
			cfg.TargetConfig.Username,
			cfg.TargetConfig.Password,
			cfg.TargetConfig.Host,
			cfg.TargetConfig.Port,
			cfg.TargetConfig.MetaSchema,
		)
		if err != nil {
			return err
		}
		if err := mysqlEngine.InitMysqlEngineDB(); err != nil {
			return err
		}
	case "reverse":
		engine, err := reverser.NewReverseEngineDB(cfg)
		if err != nil {
			return err
		}
		if err := reverser.ReverseOracleToMySQLTable(engine, cfg); err != nil {
			return err
		}
	case "run":

	default:
		return fmt.Errorf("flag [mode] can not null or value configure error")
	}
	return nil
}
