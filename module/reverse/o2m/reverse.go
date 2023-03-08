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
package o2m

import (
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/reverse"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"path/filepath"
	"strings"
	"time"
)

type Reverse struct {
	Ctx    context.Context
	Cfg    *config.Config
	Mysql  *mysql.MySQL
	Oracle *oracle.Oracle
	MetaDB *meta.Meta
}

func NewReverse(ctx context.Context, cfg *config.Config) (*Reverse, error) {
	oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
	if err != nil {
		return nil, err
	}
	mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
	if err != nil {
		return nil, err
	}
	metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MetaConfig, cfg.AppConfig.SlowlogThreshold)
	if err != nil {
		return nil, err
	}
	if cfg.ReverseConfig.DirectWrite {
		createSchema := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, cfg.MySQLConfig.SchemaName)
		_, err = mysqlDB.MySQLDB.ExecContext(ctx, createSchema)
		if err != nil {
			return nil, fmt.Errorf("error on exec target database sql [%v]: %v", createSchema, err)
		}
	}
	return &Reverse{
		Ctx:    ctx,
		Cfg:    cfg,
		Mysql:  mysqlDB,
		Oracle: oracleDB,
		MetaDB: metaDB,
	}, nil
}

func (r *Reverse) Reverse() error {
	startTime := time.Now()
	zap.L().Info("reverse table r.Oracle to mysql start",
		zap.String("schema", r.Cfg.OracleConfig.SchemaName))

	// 获取配置文件待同步表列表
	exporters, err := filterCFGTable(r.Cfg, r.Oracle)
	if err != nil {
		return err
	}

	if len(exporters) == 0 {
		zap.L().Warn("there are no table objects in the r.Oracle schema",
			zap.String("schema", r.Cfg.OracleConfig.SchemaName))
		return nil
	}

	// 判断 error_log_detail 是否存在错误记录，是否可进行 reverse
	errTotals, err := meta.NewErrorLogDetailModel(r.MetaDB).CountsErrorLogBySchema(r.Ctx, &meta.ErrorLogDetail{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TaskMode:    r.Cfg.TaskMode,
	})
	if errTotals > 0 || err != nil {
		return fmt.Errorf("reverse schema [%s] table mode [%s] task failed: %v, table [error_log_detail] exist failed error, please clear and rerunning", r.Cfg.OracleConfig.SchemaName, r.Cfg.TaskMode, err)
	}

	// 获取 oracle 数据库字符排序规则
	nlsComp, err := r.Oracle.GetOracleDBCharacterNLSCompCollation()
	if err != nil {
		return err
	}
	nlsSort, err := r.Oracle.GetOracleDBCharacterNLSSortCollation()
	if err != nil {
		return err
	}
	if _, ok := common.OracleCollationMap[common.StringUPPER(nlsComp)]; !ok {
		return fmt.Errorf("r.Oracle db nls comp [%s] , mysql db isn't support", nlsComp)
	}
	if _, ok := common.OracleCollationMap[common.StringUPPER(nlsSort)]; !ok {
		return fmt.Errorf("r.Oracle db nls sort [%s] , mysql db isn't support", nlsSort)
	}

	if !strings.EqualFold(nlsSort, nlsComp) {
		return fmt.Errorf("r.Oracle db nls_sort [%s] and nls_comp [%s] isn't different, need be equal; because mysql db isn't support", nlsSort, nlsComp)
	}

	// oracle 版本是否可指定表、字段 collation
	// oracle db nls_sort/nls_comp 值需要相等，USING_NLS_COMP 值取 nls_comp
	oracleDBVersion, err := r.Oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}

	oracleCollation := false
	if common.VersionOrdinal(oracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
		oracleCollation = true
	}

	// 筛选过滤可能不支持的表类型
	partitionTables, temporaryTables, clusteredTables, materializedView, exporterTables, err := FilterOracleCompatibleTable(r.Cfg, r.Oracle, exporters)
	if err != nil {
		return err
	}

	// 获取规则
	ruleTime := time.Now()
	tableNameRuleMap, tableColumnRuleMap, tableDefaultRuleMap, err := IChanger(&Change{
		Ctx:              r.Ctx,
		DBTypeS:          r.Cfg.DBTypeS,
		DBTypeT:          r.Cfg.DBTypeT,
		SourceSchemaName: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TargetSchemaName: common.StringUPPER(r.Cfg.MySQLConfig.SchemaName),
		SourceTables:     exporterTables,
		OracleCollation:  oracleCollation,
		Threads:          r.Cfg.ReverseConfig.ReverseThreads,
		Oracle:           r.Oracle,
		MetaDB:           r.MetaDB,
	})
	if err != nil {
		return err
	}
	zap.L().Warn("get all rules",
		zap.String("schema", r.Cfg.OracleConfig.SchemaName),
		zap.String("cost", time.Now().Sub(ruleTime).String()))

	// 获取 reverse 表任务列表
	tables, err := GenReverseTableTask(r, tableNameRuleMap, tableColumnRuleMap, tableDefaultRuleMap, oracleDBVersion, oracleCollation, exporterTables, nlsSort, nlsComp)
	if err != nil {
		return err
	}

	// file writer
	f, err := reverse.NewWriter(r.Cfg, r.Mysql, r.Oracle)
	if err != nil {
		return err
	}

	// schema create
	err = GenCreateSchema(f,
		common.StringUPPER(r.Cfg.OracleConfig.SchemaName), common.StringUPPER(r.Cfg.MySQLConfig.SchemaName), nlsComp, r.Cfg.ReverseConfig.DirectWrite)
	if err != nil {
		return err
	}

	// 表类型不兼容项输出
	err = GenCompatibilityTable(f, common.StringUPPER(r.Cfg.OracleConfig.SchemaName), partitionTables, temporaryTables, clusteredTables, materializedView)
	if err != nil {
		return err
	}

	// 表转换
	g := &errgroup.Group{}
	g.SetLimit(r.Cfg.ReverseConfig.ReverseThreads)

	for _, table := range tables {
		t := table
		g.Go(func() error {
			rule, err := IReader(t)
			if err != nil {
				if err = meta.NewErrorLogDetailModel(r.MetaDB).CreateErrorLog(r.Ctx, &meta.ErrorLogDetail{
					DBTypeS:     r.Cfg.DBTypeS,
					DBTypeT:     r.Cfg.DBTypeT,
					SchemaNameS: t.SourceSchemaName,
					TableNameS:  t.SourceTableName,
					SchemaNameT: t.TargetSchemaName,
					TableNameT:  t.TargetTableName,
					TaskMode:    r.Cfg.TaskMode,
					TaskStatus:  "Failed",
					InfoDetail:  t.String(),
					ErrorDetail: err.Error(),
				}); err != nil {
					zap.L().Error("reverse table r.Oracle to mysql failed",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", t.SourceTableName),
						zap.Error(
							fmt.Errorf("reader table task failed, detail see [error_log_detail], please rerunning")))

					return fmt.Errorf("reader table task failed, detail see [error_log_detail], please rerunning, error: %v", err)
				}
				return nil
			}
			ddl, err := IReverse(rule)
			if err != nil {
				if err = meta.NewErrorLogDetailModel(r.MetaDB).CreateErrorLog(r.Ctx, &meta.ErrorLogDetail{
					DBTypeS:     r.Cfg.DBTypeS,
					DBTypeT:     r.Cfg.DBTypeT,
					SchemaNameS: t.SourceSchemaName,
					TableNameS:  t.SourceTableName,
					SchemaNameT: t.TargetSchemaName,
					TableNameT:  t.TargetTableName,
					TaskMode:    r.Cfg.TaskMode,
					TaskStatus:  "Failed",
					InfoDetail:  t.String(),
					ErrorDetail: err.Error(),
				}); err != nil {
					zap.L().Error("reverse table r.Oracle to mysql failed",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", t.SourceTableName),
						zap.Error(
							fmt.Errorf("reverse table task failed, detail see [error_log_detail], please rerunning")))

					return fmt.Errorf("reverse table task failed, detail see [error_log_detail], please rerunning, error: %v", err)
				}
				return nil
			}

			err = IWriter(f, ddl)
			if err != nil {
				if err = meta.NewErrorLogDetailModel(r.MetaDB).CreateErrorLog(r.Ctx, &meta.ErrorLogDetail{
					DBTypeS:     r.Cfg.DBTypeS,
					DBTypeT:     r.Cfg.DBTypeT,
					SchemaNameS: t.SourceSchemaName,
					TableNameS:  t.SourceTableName,
					SchemaNameT: t.TargetSchemaName,
					TableNameT:  t.TargetTableName,
					TaskMode:    r.Cfg.TaskMode,
					TaskStatus:  "Failed",
					InfoDetail:  t.String(),
					ErrorDetail: err.Error(),
				}); err != nil {
					zap.L().Error("reverse table r.Oracle to mysql failed",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", t.SourceTableName),
						zap.Error(
							fmt.Errorf("writer table task failed, detail see [error_log_detail], please rerunning")))

					return fmt.Errorf("writer table task failed, detail see [error_log_detail], please rerunning, error: %v", err)
				}
				return nil
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	errTotals, err = meta.NewErrorLogDetailModel(r.MetaDB).CountsErrorLogBySchema(r.Ctx, &meta.ErrorLogDetail{
		DBTypeS:     r.Cfg.DBTypeS,
		DBTypeT:     r.Cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
		TaskMode:    r.Cfg.TaskMode,
	})
	if err != nil {
		return err
	}

	endTime := time.Now()
	if !r.Cfg.ReverseConfig.DirectWrite {
		zap.L().Info("reverse", zap.String("create table and index output", filepath.Join(r.Cfg.ReverseConfig.DDLReverseDir,
			fmt.Sprintf("reverse_%s.sql", r.Cfg.OracleConfig.SchemaName))))
	}
	zap.L().Info("compatibility", zap.String("maybe exist compatibility output", filepath.Join(r.Cfg.ReverseConfig.DDLCompatibleDir,
		fmt.Sprintf("compatibility_%s.sql", r.Cfg.OracleConfig.SchemaName))))
	if errTotals == 0 {
		zap.L().Info("reverse table oracle to mysql finished",
			zap.Int("table totals", len(exporters)),
			zap.Int("reverse totals", len(tables)),
			zap.Int("reverse success", len(tables)),
			zap.Int64("reverse failed", errTotals),
			zap.String("cost", endTime.Sub(startTime).String()))
	} else {
		zap.L().Warn("reverse table oracle to mysql finished",
			zap.Int("table totals", len(exporters)),
			zap.Int("reverse totals", len(tables)),
			zap.Int("reverse success", len(tables)-int(errTotals)),
			zap.Int64("reverse failed", errTotals),
			zap.String("failed tips", "failed detail, please see table [error_log_detail]"),
			zap.String("cost", endTime.Sub(startTime).String()))
	}
	return nil
}
