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
	"database/sql"
	"fmt"
	"strings"

	"github.com/wentaojin/transferdb/pkg/prepare"

	"github.com/wentaojin/transferdb/pkg/taskflow"

	"github.com/wentaojin/transferdb/pkg/config"

	"github.com/wentaojin/transferdb/pkg/reverser"

	"github.com/wentaojin/transferdb/db"
)

// 程序运行
func Run(cfg *config.CfgFile, mode string) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "cost":
		// todo: 收集评估改造成本
	case "prepare":
		// 初始化程序表结构 - only prepare 阶段
		if err := prepare.TransferDBEnvPrepare(cfg); err != nil {
			return err
		}
	case "reverse":
		engine, err := NewEngineDB(cfg)
		if err != nil {
			return err
		}
		if err := reverser.ReverseOracleToMySQLTable(engine, cfg); err != nil {
			return err
		}
	case "check":
		// todo: 校验上下游数据库表结构
	case "full":
		engine, err := NewEngineDB(cfg)
		if err != nil {
			return err
		}
		if err := taskflow.LoaderOracleTableFullRecordToMySQLByFullMode(cfg, engine); err != nil {
			return err
		}
	case "all":
		engine, err := NewEngineDB(cfg)
		if err != nil {
			return err
		}
		if err := taskflow.SyncOracleTableAllRecordToMySQLByAllMode(cfg, engine); err != nil {
			return err
		}
	default:
		return fmt.Errorf("flag [mode] can not null or value configure error")
	}
	return nil
}

// 数据库引擎初始化
func NewEngineDB(cfg *config.CfgFile) (*db.Engine, error) {
	var (
		engine *db.Engine
		oraDB  *sql.DB
		err    error
	)
	oraDB, err = db.NewOracleDBEngine(cfg.SourceConfig)
	if err != nil {
		return engine, err
	}
	engine, err = db.NewMySQLEngineGeneralDB(
		cfg.TargetConfig,
		cfg.AppConfig.SlowlogThreshold)
	if err != nil {
		return engine, err
	}
	engine.OracleDB = oraDB
	return engine, nil
}
