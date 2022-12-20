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
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/module/assess"
	assessO2M "github.com/wentaojin/transferdb/module/assess/o2m"
	"github.com/wentaojin/transferdb/module/check"
	"github.com/wentaojin/transferdb/module/check/o2m"
	"github.com/wentaojin/transferdb/module/compare"
	compareO2M "github.com/wentaojin/transferdb/module/compare/o2m"
	"github.com/wentaojin/transferdb/module/csv"
	csvO2M "github.com/wentaojin/transferdb/module/csv/o2m"
	"github.com/wentaojin/transferdb/module/migrate"
	migrateO2M "github.com/wentaojin/transferdb/module/migrate/o2m"
	"github.com/wentaojin/transferdb/module/prepare"
	"github.com/wentaojin/transferdb/module/reverse"
	reverseM2O "github.com/wentaojin/transferdb/module/reverse/m2o"
	reverseO2M "github.com/wentaojin/transferdb/module/reverse/o2m"

	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"strings"
)

// 程序运行
func Run(ctx context.Context, cfg *config.Config) error {
	switch strings.ToLower(strings.TrimSpace(cfg.Mode)) {
	case "assess":
		// 收集评估改造成本
		metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
		if err != nil {
			return err
		}
		oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
		if err != nil {
			return err
		}
		err = assess.TAssess(assessO2M.NewAssess(ctx, cfg, metaDB, oracleDB))
		if err != nil {
			return err
		}
	case "prepare":
		// 表结构转换 - only prepare 阶段
		err := prepare.TPrepare(ctx, cfg)
		if err != nil {
			return err
		}
	case "reverseo2m":
		// 表结构转换 - reverse 阶段
		// O2M
		metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
		if err != nil {
			return err
		}
		oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
		if err != nil {
			return err
		}
		mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
		if err != nil {
			return err
		}

		err = reverse.IReverse(reverseO2M.NewO2MReverse(
			ctx, cfg, mysqlDB, oracleDB, metaDB))
		if err != nil {
			return err
		}
	case "reversem2o":
		// 表结构转换 - reverse 阶段
		// M2O
		metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
		if err != nil {
			return err
		}
		oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
		if err != nil {
			return err
		}
		mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
		if err != nil {
			return err
		}

		err = reverse.IReverse(reverseM2O.NewM2OReverse(ctx, cfg,
			mysqlDB, oracleDB, metaDB))
		if err != nil {
			return err
		}
	case "check":
		// 表结构校验 - 上下游
		metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
		if err != nil {
			return err
		}
		oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
		if err != nil {
			return err
		}
		mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
		if err != nil {
			return err
		}

		err = check.ICheck(o2m.NewO2MCheck(ctx, cfg,
			oracleDB, mysqlDB, metaDB))
		if err != nil {
			return err
		}
	case "compare":
		// 数据校验 - 以上游为准
		metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
		if err != nil {
			return err
		}
		oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
		if err != nil {
			return err
		}
		mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
		if err != nil {
			return err
		}
		err = compare.ICompare(compareO2M.NewO2MCompare(ctx, cfg, oracleDB, mysqlDB, metaDB))
		if err != nil {
			return err
		}
	case "csv":
		// csv 全量数据导出
		metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
		if err != nil {
			return err
		}
		oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
		if err != nil {
			return err
		}
		mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
		if err != nil {
			return err
		}
		err = csv.ICSVer(csvO2M.NewO2MCSVer(ctx, cfg,
			oracleDB, mysqlDB, metaDB))
		if err != nil {
			return err
		}
	case "full":
		// 全量数据 ETL 非一致性（基于某个时间点，而是直接基于现有 SCN）抽取，离线环境提供与原库一致性
		metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
		if err != nil {
			return err
		}
		oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
		if err != nil {
			return err
		}
		mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
		if err != nil {
			return err
		}
		err = migrate.IMigrateFull(migrateO2M.NewO2MFuller(ctx, cfg, oracleDB, mysqlDB, metaDB))
		if err != nil {
			return err
		}
	case "all":
		// 全量 + 增量数据同步阶段 - logminer
		metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
		if err != nil {
			return err
		}
		oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
		if err != nil {
			return err
		}
		mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
		if err != nil {
			return err
		}
		err = migrate.IMigrateIncr(migrateO2M.NewO2MIncr(ctx, cfg, oracleDB, mysqlDB, metaDB))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("flag [mode] can not null or value configure error")
	}
	return nil
}
