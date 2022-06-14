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
	"github.com/wentaojin/transferdb/pkg/diff"
	"strings"
	"time"

	"github.com/wentaojin/transferdb/pkg/csv"

	"github.com/wentaojin/transferdb/pkg/cost"

	"github.com/wentaojin/transferdb/pkg/check"

	"github.com/wentaojin/transferdb/service"

	"github.com/wentaojin/transferdb/pkg/prepare"

	"github.com/wentaojin/transferdb/pkg/taskflow"

	"github.com/wentaojin/transferdb/pkg/reverser"
)

const (
	mysqlMaxConn         = 1024
	mysqlConnMaxLifeTime = 30 * time.Second
	mysqlConnMaxIdleTime = 1 * time.Minute
)

// 程序运行
func Run(cfg *service.CfgFile, mode string) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "gather":
		// 收集评估改造成本
		engine, err := NewOracleDBEngine(cfg.SourceConfig)
		if err != nil {
			return err
		}
		if err = cost.OracleMigrateMySQLCostEvaluate(&service.Engine{OracleDB: engine}, cfg); err != nil {
			return err
		}
	case "prepare":
		// 表结构转换 - only prepare 阶段
		engine, err := NewMySQLEnginePrepareDB(cfg.TargetConfig, cfg.AppConfig.SlowlogThreshold, mysqlMaxConn)
		if err != nil {
			return err
		}
		if err = prepare.TransferDBEnvPrepare(engine); err != nil {
			return err
		}
	case "reverse":
		// 表结构转换 - reverse 阶段
		engine, err := NewEngineDB(cfg.SourceConfig, cfg.TargetConfig, cfg.AppConfig.SlowlogThreshold, mysqlMaxConn)
		if err != nil {
			return err
		}
		if err = reverser.ReverseOracleToMySQLTable(engine, cfg); err != nil {
			return err
		}
	case "check":
		// 表结构校验 - 上下游
		engine, err := NewEngineDB(cfg.SourceConfig, cfg.TargetConfig, cfg.AppConfig.SlowlogThreshold, mysqlMaxConn)
		if err != nil {
			return err
		}
		if err = check.OracleTableToMySQLMappingCheck(engine, cfg); err != nil {
			return err
		}
	case "diff":
		// 数据校验 - 以上游为准
		engine, err := NewEngineDB(cfg.SourceConfig, cfg.TargetConfig, cfg.AppConfig.SlowlogThreshold, mysqlMaxConn)
		if err != nil {
			return err
		}
		if err = diff.OracleDiffMySQLTable(engine, cfg); err != nil {
			return err
		}
	case "full":
		// 全量数据 ETL 非一致性（基于某个时间点，而是直接基于现有 SCN）抽取，离线环境提供与原库一致性
		engine, err := NewEngineDB(
			cfg.SourceConfig, cfg.TargetConfig, cfg.AppConfig.SlowlogThreshold,
			cfg.FullConfig.TableThreads*cfg.FullConfig.SQLThreads*cfg.FullConfig.ApplyThreads)
		if err != nil {
			return err
		}
		if err = taskflow.FullSyncOracleTableRecordToMySQL(cfg, engine); err != nil {
			return err
		}
	case "csv":
		// csv 全量数据导出
		engine, err := NewEngineDB(cfg.SourceConfig, cfg.TargetConfig, cfg.AppConfig.SlowlogThreshold, mysqlMaxConn)
		if err != nil {
			return err
		}
		if err := csv.FullCSVOracleTableRecordToMySQL(cfg, engine); err != nil {
			return err
		}
	case "all":
		// 全量 + 增量数据同步阶段 - logminer
		engine, err := NewEngineDB(
			cfg.SourceConfig, cfg.TargetConfig, cfg.AppConfig.SlowlogThreshold,
			cfg.FullConfig.TableThreads*cfg.FullConfig.SQLThreads*cfg.FullConfig.ApplyThreads)
		if err != nil {
			return err
		}
		if err = taskflow.IncrementSyncOracleTableRecordToMySQL(cfg, engine); err != nil {
			return err
		}
	default:
		return fmt.Errorf("flag [mode] can not null or value configure error")
	}
	return nil
}

// 数据库引擎初始化
func NewEngineDB(sourceCfg service.SourceConfig, targetCfg service.TargetConfig, slowlogThreshold, mysqlMaxOpenConn int) (*service.Engine, error) {
	var (
		engine *service.Engine
		oraDB  *sql.DB
		err    error
	)
	oraDB, err = NewOracleDBEngine(sourceCfg)
	if err != nil {
		return engine, err
	}
	engine, err = NewMySQLEngineGeneralDB(
		targetCfg,
		slowlogThreshold,
		mysqlMaxOpenConn)
	if err != nil {
		return engine, err
	}
	engine.OracleDB = oraDB
	return engine, nil
}
