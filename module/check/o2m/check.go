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
	"github.com/wentaojin/transferdb/module/check"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"path/filepath"
	"strings"
	"time"
)

type Check struct {
	ctx    context.Context
	cfg    *config.Config
	mysql  *mysql.MySQL
	oracle *oracle.Oracle
	metaDB *meta.Meta
}

func NewCheck(ctx context.Context, cfg *config.Config) (*Check, error) {
	metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
	if err != nil {
		return nil, err
	}
	oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
	if err != nil {
		return nil, err
	}
	mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
	if err != nil {
		return nil, err
	}
	return &Check{
		ctx:    ctx,
		cfg:    cfg,
		mysql:  mysqlDB,
		oracle: oracleDB,
		metaDB: metaDB,
	}, nil
}

func (r *Check) Check() error {
	startTime := time.Now()
	zap.L().Info("check oracle and mysql table start",
		zap.String("oracleSchema", r.cfg.OracleConfig.SchemaName),
		zap.String("mysqlSchema", r.cfg.MySQLConfig.SchemaName))

	tablesByCfg, err := filterCFGTable(r.cfg, r.oracle)
	if err != nil {
		return err
	}

	// 判断下游数据库是否存在 oracle 表
	mysqlTables, err := r.mysql.GetMySQLTable(r.cfg.MySQLConfig.SchemaName)
	if err != nil {
		return err
	}
	ok, noExistTables := common.IsSubsetString(mysqlTables, tablesByCfg)
	if !ok {
		return fmt.Errorf("oracle tables %v isn't exist in the mysqldb schema [%v], please create", noExistTables, r.cfg.MySQLConfig.SchemaName)
	}

	// 清理非当前任务 SUCCESS 表元数据记录 wait_sync_meta (用于统计 SUCCESS 准备)
	// 例如：当前任务表 A/B，之前任务表 A/C (SUCCESS)，清理元数据 C，对于表 A 任务 Skip 忽略处理，除非手工清理表 A
	tablesByMeta, err := meta.NewWaitSyncMetaModel(r.metaDB).DetailWaitSyncMetaSuccessTables(r.ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.cfg.DBTypeS,
		DBTypeT:     r.cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		TaskMode:    r.cfg.TaskMode,
		TaskStatus:  common.TaskStatusSuccess,
	})
	if err != nil {
		return err
	}

	clearTables := common.FilterDifferenceStringItems(tablesByMeta, tablesByCfg)
	interTables := common.FilterIntersectionStringItems(tablesByMeta, tablesByCfg)
	if len(clearTables) > 0 {
		err = meta.NewWaitSyncMetaModel(r.metaDB).DeleteWaitSyncMetaSuccessTables(r.ctx, &meta.WaitSyncMeta{
			DBTypeS:     r.cfg.DBTypeS,
			DBTypeT:     r.cfg.DBTypeT,
			SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
			TaskMode:    r.cfg.TaskMode,
			TaskStatus:  common.TaskStatusSuccess,
		}, clearTables)
		if err != nil {
			return err
		}
	}
	zap.L().Warn("non-task table clear",
		zap.Strings("clear tables", clearTables),
		zap.Strings("intersection tables", interTables),
		zap.Int("clear totals", len(clearTables)),
		zap.Int("intersection total", len(interTables)))

	// 判断 check_error_detail 是否存在错误记录，是否可进行 check
	errTotals, err := meta.NewErrorLogDetailModel(r.metaDB).CountsErrorLogBySchema(r.ctx, &meta.ErrorLogDetail{
		DBTypeS:     r.cfg.DBTypeS,
		DBTypeT:     r.cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		TaskMode:    r.cfg.TaskMode,
	})

	if errTotals > 0 || err != nil {
		return fmt.Errorf(`check schema [%s] mode [%s] table task failed: %v, table [check_error_detail] exist failed error, please firstly check log and deal, secondly clear table [check_error_detail], thirdly update meta table [wait_sync_meta] column [task_status] table status WAITING (Need UPPER), finally rerunning`, strings.ToUpper(r.cfg.OracleConfig.SchemaName), r.cfg.TaskMode, err)
	}

	// 判断并记录待同步表列表
	for _, tableName := range tablesByCfg {
		waitSyncMetas, err := meta.NewWaitSyncMetaModel(r.metaDB).DetailWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
			DBTypeS:     r.cfg.DBTypeS,
			DBTypeT:     r.cfg.DBTypeT,
			SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
			TableNameS:  tableName,
			TaskMode:    r.cfg.TaskMode,
		})
		if err != nil {
			return err
		}
		if len(waitSyncMetas) == 0 {
			err = meta.NewWaitSyncMetaModel(r.metaDB).CreateWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
				DBTypeS:     r.cfg.DBTypeS,
				DBTypeT:     r.cfg.DBTypeT,
				SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
				TableNameS:  common.StringUPPER(tableName),
				TaskMode:    r.cfg.TaskMode,
				TaskStatus:  common.TaskStatusWaiting,
			})
			if err != nil {
				return err
			}
		}
	}

	// 获取待处理任务
	waitSyncMetas, err := meta.NewWaitSyncMetaModel(r.metaDB).DetailWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.cfg.DBTypeS,
		DBTypeT:     r.cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		TaskMode:    r.cfg.TaskMode,
		TaskStatus:  common.TaskStatusWaiting,
	})
	if err != nil {
		return err
	}

	// 环境信息
	beginTime := time.Now()
	oracleDBCharacterSet, err := r.oracle.GetOracleDBCharacterSet()
	if err != nil {
		return err
	}
	if _, ok := common.OracleDBCharacterSetMap[strings.Split(oracleDBCharacterSet, ".")[1]]; !ok {
		return fmt.Errorf("oracle db character set [%v] isn't support", oracleDBCharacterSet)
	}

	// oracle db collation
	nlsSort, err := r.oracle.GetOracleDBCharacterNLSSortCollation()
	if err != nil {
		return err
	}
	nlsComp, err := r.oracle.GetOracleDBCharacterNLSCompCollation()
	if err != nil {
		return err
	}
	if _, ok := common.OracleCollationMap[strings.ToUpper(nlsSort)]; !ok {
		return fmt.Errorf("oracle db nls sort [%s] isn't support", nlsSort)
	}
	if _, ok := common.OracleCollationMap[strings.ToUpper(nlsComp)]; !ok {
		return fmt.Errorf("oracle db nls comp [%s] isn't support", nlsComp)
	}
	if strings.ToUpper(nlsSort) != strings.ToUpper(nlsComp) {
		return fmt.Errorf("oracle db nls_sort [%s] and nls_comp [%s] isn't different, need be equal; because mysql db isn't support", nlsSort, nlsComp)
	}

	// oracle 版本是否存在 collation
	oracleDBVersion, err := r.oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}

	oracleDBCollation := false
	if common.VersionOrdinal(oracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
		oracleDBCollation = true
	}
	finishTime := time.Now()
	zap.L().Info("get oracle db character and version finished",
		zap.String("schema", r.cfg.OracleConfig.SchemaName),
		zap.String("db version", oracleDBVersion),
		zap.String("db character", oracleDBCharacterSet),
		zap.Int("table totals", len(waitSyncMetas)),
		zap.Bool("table collation", oracleDBCollation),
		zap.String("cost", finishTime.Sub(beginTime).String()))

	var (
		oracleTableCollation  map[string]string
		oracleSchemaCollation string
	)

	if oracleDBCollation {
		beginTime = time.Now()
		oracleSchemaCollation, err = r.oracle.GetOracleSchemaCollation(strings.ToUpper(r.cfg.OracleConfig.SchemaName))
		if err != nil {
			return err
		}
		oracleTableCollation, err = r.oracle.GetOracleSchemaTableCollation(strings.ToUpper(r.cfg.OracleConfig.SchemaName), oracleSchemaCollation)
		if err != nil {
			return err
		}
		finishTime = time.Now()
		zap.L().Info("get oracle schema and table collation finished",
			zap.String("schema", r.cfg.OracleConfig.SchemaName),
			zap.String("db version", oracleDBVersion),
			zap.String("db character", oracleDBCharacterSet),
			zap.Int("table totals", len(waitSyncMetas)),
			zap.Bool("table collation", oracleDBCollation),
			zap.String("cost", finishTime.Sub(beginTime).String()))
	}

	// 获取表名自定义规则
	tableNameRules, err := meta.NewTableNameRuleModel(r.metaDB).DetailTableNameRule(r.ctx, &meta.TableNameRule{
		DBTypeS:     r.cfg.DBTypeS,
		DBTypeT:     r.cfg.DBTypeT,
		SchemaNameS: r.cfg.OracleConfig.SchemaName,
		SchemaNameT: r.cfg.MySQLConfig.SchemaName,
	})
	if err != nil {
		return err
	}
	tableNameRuleMap := make(map[string]string)

	if len(tableNameRules) > 0 {
		for _, tr := range tableNameRules {
			tableNameRuleMap[common.StringUPPER(tr.TableNameS)] = common.StringUPPER(tr.TableNameT)
		}
	}

	// 任务检查表
	tasks := GenCheckTaskTable(r.cfg.OracleConfig.SchemaName, r.cfg.MySQLConfig.SchemaName, oracleDBCharacterSet,
		nlsSort, nlsComp, oracleTableCollation, oracleSchemaCollation, oracleDBCollation,
		r.cfg.MySQLConfig.DBType, r.oracle, r.mysql, tableNameRuleMap, waitSyncMetas)

	err = common.PathExist(r.cfg.CheckConfig.CheckSQLDir)
	if err != nil {
		return err
	}

	checkFile := filepath.Join(r.cfg.CheckConfig.CheckSQLDir, fmt.Sprintf("check_%s.sql", r.cfg.OracleConfig.SchemaName))

	// file writer
	f, err := check.NewWriter(checkFile)
	if err != nil {
		return err
	}

	g := &errgroup.Group{}
	g.SetLimit(r.cfg.CheckConfig.CheckThreads)

	for _, task := range tasks {
		t := task
		g.Go(func() error {
			oracleTableInfo, err := t.GenOracleTable()
			if err != nil {
				return err
			}
			mysqlTableInfo, mysqlDBVersion, err := t.GenMySQLTable()
			if err != nil {
				return err
			}

			err = meta.NewWaitSyncMetaModel(r.metaDB).UpdateWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
				DBTypeS:     r.cfg.DBTypeS,
				DBTypeT:     r.cfg.DBTypeT,
				SchemaNameS: t.SourceSchemaName,
				TableNameS:  t.SourceTableName,
				TaskMode:    r.cfg.TaskMode,
			}, map[string]interface{}{
				"TaskStatus": common.TaskStatusRunning,
			})
			if err != nil {
				return err
			}
			err = NewChecker(r.ctx, oracleTableInfo, mysqlTableInfo,
				r.cfg.DBTypeS, r.cfg.DBTypeT, mysqlDBVersion, r.cfg.MySQLConfig.DBType, r.metaDB).Writer(f)
			if err != nil {
				// skip error and continue
				errMeta := meta.NewCommonModel(r.metaDB).CreateErrorDetailAndUpdateWaitSyncMetaTaskStatus(r.ctx, &meta.ErrorLogDetail{
					DBTypeS:     r.cfg.DBTypeS,
					DBTypeT:     r.cfg.DBTypeT,
					SchemaNameS: t.SourceSchemaName,
					TableNameS:  t.SourceTableName,
					SchemaNameT: t.TargetSchemaName,
					TableNameT:  t.TargetTableName,
					TaskMode:    r.cfg.TaskMode,
					TaskStatus:  common.TaskStatusFailed,
					InfoDetail:  t.String(),
					ErrorDetail: err.Error(),
				}, &meta.WaitSyncMeta{
					DBTypeS:     r.cfg.DBTypeS,
					DBTypeT:     r.cfg.DBTypeT,
					SchemaNameS: t.SourceSchemaName,
					TableNameS:  t.SourceTableName,
					TaskMode:    r.cfg.TaskMode,
					TaskStatus:  common.TaskStatusFailed,
				})
				if errMeta != nil {
					return errMeta
				}
			} else {
				errMeta := meta.NewWaitSyncMetaModel(r.metaDB).UpdateWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
					DBTypeS:     r.cfg.DBTypeS,
					DBTypeT:     r.cfg.DBTypeT,
					SchemaNameS: t.SourceSchemaName,
					TableNameS:  t.SourceTableName,
					TaskMode:    r.cfg.TaskMode,
				}, map[string]interface{}{
					"TaskStatus": common.TaskStatusSuccess,
				})
				if errMeta != nil {
					return errMeta
				}
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	if err = f.Close(); err != nil {
		return err
	}

	// 任务详情
	succTotals, err := meta.NewWaitSyncMetaModel(r.metaDB).DetailWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.cfg.DBTypeS,
		DBTypeT:     r.cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		TaskMode:    r.cfg.TaskMode,
		TaskStatus:  common.TaskStatusSuccess,
	})
	if err != nil {
		return err
	}
	failedTotals, err := meta.NewWaitSyncMetaModel(r.metaDB).DetailWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.cfg.DBTypeS,
		DBTypeT:     r.cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		TaskMode:    r.cfg.TaskMode,
		TaskStatus:  common.TaskStatusFailed,
	})
	if err != nil {
		return err
	}

	zap.L().Info("check", zap.String("output", filepath.Join(r.cfg.CheckConfig.CheckSQLDir, fmt.Sprintf("check_%s.sql", r.cfg.OracleConfig.SchemaName))))
	if len(failedTotals) == 0 {
		zap.L().Info("check table oracle to mysql finished",
			zap.Int("table totals", len(waitSyncMetas)),
			zap.Int("table success", len(succTotals)),
			zap.Int("table failed", 0),
			zap.String("cost", time.Now().Sub(startTime).String()))
	} else {
		zap.L().Warn("check table oracle to mysql finished",
			zap.Int("table totals", len(waitSyncMetas)),
			zap.Int("table success", len(succTotals)),
			zap.Int("check failed", len(failedTotals)),
			zap.String("failed tips", "failed detail, please see table [error_log_detail]"),
			zap.String("cost", time.Now().Sub(startTime).String()))
	}

	return nil
}
