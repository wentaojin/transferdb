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

	"github.com/wentaojin/transferdb/pkg/check"

	"github.com/wentaojin/transferdb/service"

	"github.com/wentaojin/transferdb/pkg/prepare"

	"github.com/wentaojin/transferdb/pkg/taskflow"

	"github.com/wentaojin/transferdb/pkg/reverser"
)

// 程序运行
func Run(cfg *service.CfgFile, mode string) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "cost":
		// todo: 收集评估改造成本
	case "prepare":
		// 表结构转换 - only prepare 阶段
		engine, err := NewMySQLEnginePrepareDB(
			cfg.TargetConfig,
			cfg.AppConfig.SlowlogThreshold,
		)
		if err != nil {
			return err
		}
		if err := prepare.TransferDBEnvPrepare(engine); err != nil {
			return err
		}
	case "reverse":
		// 表结构转换 - reverse 阶段
		engine, err := NewEngineDB(cfg)
		if err != nil {
			return err
		}
		if err := reverser.ReverseOracleToMySQLTable(engine, cfg); err != nil {
			return err
		}
	case "check":
		// 表结构校验 - 上下游
		engine, err := NewEngineDB(cfg)
		if err != nil {
			return err
		}
		if err := check.CheckOracleTableToMySQLMapping(engine, cfg); err != nil {
			return err
		}
	case "full":
		// 全量数据 ETL 非一致性抽取阶段
		engine, err := NewEngineDB(cfg)
		if err != nil {
			return err
		}
		if err := taskflow.LoaderOracleTableFullRecordToMySQLByFullMode(cfg, engine); err != nil {
			return err
		}
	case "all":
		// 全量 + 增量数据同步阶段 - logminer
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
func NewEngineDB(cfg *service.CfgFile) (*service.Engine, error) {
	var (
		engine *service.Engine
		oraDB  *sql.DB
		err    error
	)
	oraDB, err = NewOracleDBEngine(cfg.SourceConfig)
	if err != nil {
		return engine, err
	}
	engine, err = NewMySQLEngineGeneralDB(
		cfg.TargetConfig,
		cfg.AppConfig.SlowlogThreshold)
	if err != nil {
		return engine, err
	}
	engine.OracleDB = oraDB
	return engine, nil
}
