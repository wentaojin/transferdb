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
	"github.com/wentaojin/transferdb/module/compare"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"path/filepath"
	"strings"
	"time"
)

type O2M struct {
	ctx    context.Context
	cfg    *config.Config
	oracle *oracle.Oracle
	mysql  *mysql.MySQL
	metaDB *meta.Meta
}

func NewCompare(ctx context.Context, cfg *config.Config) (*O2M, error) {
	oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
	if err != nil {
		return nil, err
	}
	mysqlDB, err := mysql.NewMySQLDBEngine(ctx, cfg.MySQLConfig)
	if err != nil {
		return nil, err
	}
	metaDB, err := meta.NewMetaDBEngine(ctx, cfg.MySQLConfig, cfg.AppConfig.SlowlogThreshold)
	if err != nil {
		return nil, err
	}
	return &O2M{
		ctx:    ctx,
		cfg:    cfg,
		oracle: oracleDB,
		mysql:  mysqlDB,
		metaDB: metaDB,
	}, nil
}

func (r *O2M) NewCompare() error {
	startTime := time.Now()
	zap.L().Info("diff table oracle to mysql start",
		zap.String("schema", r.cfg.OracleConfig.SchemaName))

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oraDBVersion, err := r.oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if common.VersionOrdinal(oraDBVersion) < common.VersionOrdinal(common.RequireOracleDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using transferdb tools", oraDBVersion)
	}

	// 获取配置文件待同步表列表
	exporters, err := filterCFGTable(r.cfg, r.oracle)
	if err != nil {
		return err
	}

	if len(exporters) == 0 {
		zap.L().Warn("there are no table objects in the oracle schema",
			zap.String("schema", r.cfg.OracleConfig.SchemaName))
		return nil
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

	clearTables := common.FilterDifferenceStringItems(tablesByMeta, exporters)
	interTables := common.FilterIntersectionStringItems(tablesByMeta, exporters)
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

	// 判断 [wait_sync_meta] 是否存在错误记录，是否可进行 COMPARE
	errTotals, err := meta.NewWaitSyncMetaModel(r.metaDB).CountsErrWaitSyncMetaBySchema(r.ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.cfg.DBTypeS,
		DBTypeT:     r.cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		TaskMode:    r.cfg.TaskMode,
		TaskStatus:  common.TaskStatusFailed,
	})
	if errTotals > 0 || err != nil {
		return fmt.Errorf(`compare schema [%s] mode [%s] table task failed: %v, meta table [wait_sync_meta] exist failed error, please firstly check log and deal, secondly clear or update meta table [wait_sync_meta] column [task_status] table status WAITING (Need UPPER), thirdly clear meta table [data_compare_meta] error table record, finally rerunning`, strings.ToUpper(r.cfg.OracleConfig.SchemaName), r.cfg.TaskMode, err)
	}

	// 判断并记录待同步表列表
	for _, tableName := range exporters {
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
				DBTypeS:        r.cfg.DBTypeS,
				DBTypeT:        r.cfg.DBTypeT,
				SchemaNameS:    common.StringUPPER(r.cfg.OracleConfig.SchemaName),
				TableNameS:     common.StringUPPER(tableName),
				TaskMode:       r.cfg.TaskMode,
				TaskStatus:     common.TaskStatusWaiting,
				GlobalScnS:     common.TaskTableDefaultSourceGlobalSCN,
				ChunkTotalNums: common.TaskTableDefaultSplitChunkNums,
			})
			if err != nil {
				return err
			}
		}
	}

	// 关于全量断点恢复
	if !r.cfg.DiffConfig.EnableCheckpoint {
		err = meta.NewDataCompareMetaModel(r.metaDB).TruncateDataCompareMeta(r.ctx)
		if err != nil {
			return err
		}

		for _, tableName := range exporters {
			err = meta.NewWaitSyncMetaModel(r.metaDB).DeleteWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
				DBTypeS:     r.cfg.DBTypeS,
				DBTypeT:     r.cfg.DBTypeT,
				SchemaNameS: r.cfg.OracleConfig.SchemaName,
				TableNameS:  tableName,
				TaskMode:    r.cfg.TaskMode,
			})
			if err != nil {
				return err
			}

			// 判断并记录待同步表列表
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
					DBTypeS:        r.cfg.DBTypeS,
					DBTypeT:        r.cfg.DBTypeT,
					SchemaNameS:    common.StringUPPER(r.cfg.OracleConfig.SchemaName),
					TableNameS:     common.StringUPPER(tableName),
					TaskStatus:     common.TaskStatusWaiting,
					GlobalScnS:     common.TaskTableDefaultSourceGlobalSCN,
					ChunkTotalNums: common.TaskTableDefaultSplitChunkNums,
				})
				if err != nil {
					return err
				}
			}
		}
	}

	// 获取等待同步以及未同步完成的表列表
	var (
		waitSyncTableMetas []meta.WaitSyncMeta
		waitSyncTables     []string
	)

	waitSyncDetails, err := meta.NewWaitSyncMetaModel(r.metaDB).DetailWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
		DBTypeS:        r.cfg.DBTypeS,
		DBTypeT:        r.cfg.DBTypeT,
		SchemaNameS:    common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		TaskMode:       r.cfg.TaskMode,
		TaskStatus:     common.TaskStatusWaiting,
		GlobalScnS:     common.TaskTableDefaultSourceGlobalSCN,
		ChunkTotalNums: common.TaskTableDefaultSplitChunkNums,
	})
	if err != nil {
		return err
	}
	waitSyncTableMetas = waitSyncDetails
	if len(waitSyncTableMetas) > 0 {
		for _, table := range waitSyncTableMetas {
			waitSyncTables = append(waitSyncTables, common.StringUPPER(table.TableNameS))
		}
	}

	// 判断未同步完成的表列表能否断点续传
	var (
		partSyncTableMetas []meta.WaitSyncMeta
		partSyncTables     []string
		panicTblFullSlice  []string
	)
	partSyncDetails, err := meta.NewWaitSyncMetaModel(r.metaDB).QueryWaitSyncMetaByPartTask(r.ctx, &meta.WaitSyncMeta{
		DBTypeS:     r.cfg.DBTypeS,
		DBTypeT:     r.cfg.DBTypeT,
		SchemaNameS: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		TaskMode:    r.cfg.TaskMode,
		TaskStatus:  common.TaskStatusRunning,
	})
	if err != nil {
		return err
	}
	partSyncTableMetas = partSyncDetails
	if len(partSyncTableMetas) > 0 {
		for _, t := range partSyncTableMetas {
			partSyncTables = append(partSyncTables, common.StringUPPER(t.TableNameS))
		}
	}

	waitCompareChunkTables, err := meta.NewDataCompareMetaModel(r.metaDB).DistinctDataCompareMetaTableNameSByTaskStatus(r.ctx, &meta.DataCompareMeta{
		DBTypeS:     r.cfg.DBTypeS,
		DBTypeT:     r.cfg.DBTypeT,
		SchemaNameS: r.cfg.OracleConfig.SchemaName,
		TaskMode:    r.cfg.TaskMode,
		TaskStatus:  common.TaskStatusWaiting,
	})
	if err != nil {
		return err
	}

	for _, partSyncTable := range partSyncDetails {
		if !common.IsContainString(waitCompareChunkTables, partSyncTable.TableNameS) {
			panicTblFullSlice = append(panicTblFullSlice, partSyncTable.TableNameS)
		} else {
			counts, err := meta.NewDataCompareMetaModel(r.metaDB).CountsDataCompareMetaByTaskTable(r.ctx, &meta.DataCompareMeta{
				DBTypeS:     r.cfg.DBTypeS,
				DBTypeT:     r.cfg.DBTypeT,
				SchemaNameS: r.cfg.OracleConfig.SchemaName,
				TableNameS:  partSyncTable.TableNameS,
				TaskMode:    r.cfg.TaskMode,
			})
			if err != nil {
				return err
			}
			if counts != partSyncTable.ChunkTotalNums {
				panicTblFullSlice = append(panicTblFullSlice, partSyncTable.TableNameS)
			}
		}
	}

	if (len(panicTblFullSlice) != 0) || (len(partSyncDetails) != len(waitCompareChunkTables)) {
		endTime := time.Now()
		zap.L().Error("all oracle table data compare error",
			zap.String("schema", r.cfg.OracleConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Int("part sync tables", len(partSyncDetails)),
			zap.Int("wait compare chunk tables", len(waitCompareChunkTables)),
			zap.Strings("panic tables", panicTblFullSlice))
		return fmt.Errorf("checkpoint isn't consistent, can't be resume, please reruning [enable-checkpoint = fase]")
	}

	// ORACLE 环境信息
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
	if !strings.EqualFold(nlsSort, nlsComp) {
		return fmt.Errorf("oracle db nls_sort [%s] and nls_comp [%s] isn't different, need be equal; because mysql db isn't support", nlsSort, nlsComp)
	}

	// oracle 版本是否存在 collation
	oracleCollation := false
	if common.VersionOrdinal(oraDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
		oracleCollation = true
	}
	finishTime := time.Now()
	zap.L().Info("get oracle db character and version finished",
		zap.String("schema", r.cfg.OracleConfig.SchemaName),
		zap.String("db version", oraDBVersion),
		zap.String("db character", oracleDBCharacterSet),
		zap.Int("table totals", len(exporters)),
		zap.Bool("table collation", oracleCollation),
		zap.String("cost", finishTime.Sub(beginTime).String()))

	// 判断下游是否存在 ORACLE 表
	var tables []string
	for _, t := range exporters {
		tables = append(tables, common.StringsBuilder("'", t, "'"))
	}
	mysqlTables, err := r.mysql.GetMySQLTableName(r.cfg.MySQLConfig.SchemaName, strings.Join(tables, ","))
	if err != nil {
		return err
	}

	diffItems := common.FilterDifferenceStringItems(exporters, mysqlTables)
	if len(diffItems) != 0 {
		return fmt.Errorf("table [%v] target db isn't exists, please create table", diffItems)
	}

	// compare 任务列表
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

	partTableTasks := NewPartCompareTableTask(r.ctx, r.cfg, partSyncTables, r.mysql, r.oracle, tableNameRuleMap)
	waitTableTasks := NewWaitCompareTableTask(r.ctx, r.cfg, waitSyncTables, oracleCollation, r.mysql, r.oracle, tableNameRuleMap)

	// 数据对比
	err = common.PathExist(r.cfg.DiffConfig.FixSqlDir)
	if err != nil {
		return err
	}

	checkFile := filepath.Join(r.cfg.DiffConfig.FixSqlDir, fmt.Sprintf("compare_%s.sql", r.cfg.OracleConfig.SchemaName))

	// file writer
	f, err := compare.NewWriter(checkFile)
	if err != nil {
		return err
	}

	// 优先存在断点的表校验
	// partTableTask -> waitTableTasks
	if len(partTableTasks) > 0 {
		err = PreTableStructCheck(r.ctx, r.cfg, r.metaDB, partSyncTables)
		if err != nil {
			return err
		}
		err = r.comparePartTableTasks(f, partTableTasks)
		if err != nil {
			return err
		}
	}
	if len(waitTableTasks) > 0 {
		err = PreTableStructCheck(r.ctx, r.cfg, r.metaDB, partSyncTables)
		if err != nil {
			return err
		}
		err = r.compareWaitTableTasks(f, waitTableTasks)
		if err != nil {
			return err
		}
	}

	err = f.Close()
	if err != nil {
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

	zap.L().Info("compare", zap.String("fix sql file output", checkFile))
	if len(failedTotals) == 0 {
		zap.L().Info("compare table oracle to mysql finished",
			zap.Int("table totals", len(exporters)),
			zap.Int("table success", len(succTotals)),
			zap.Int("table failed", len(failedTotals)),
			zap.String("cost", time.Now().Sub(startTime).String()))
	} else {
		zap.L().Warn("compare table oracle to mysql finished",
			zap.Int("table totals", len(exporters)),
			zap.Int("table success", len(succTotals)),
			zap.Int("table failed", len(failedTotals)),
			zap.String("failed tips", "failed detail, please see table [data_compare_meta]"),
			zap.String("cost", time.Now().Sub(startTime).String()))
	}
	return nil
}

func (r *O2M) comparePartTableTasks(f *compare.File, partTableTasks []*Task) error {
	for _, task := range partTableTasks {
		// 获取对比记录
		diffStartTime := time.Now()

		err := meta.NewWaitSyncMetaModel(r.metaDB).UpdateWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
			DBTypeS:     r.cfg.DBTypeS,
			DBTypeT:     r.cfg.DBTypeT,
			SchemaNameS: r.cfg.OracleConfig.SchemaName,
			TableNameS:  task.sourceTableName,
			TaskMode:    r.cfg.TaskMode,
		}, map[string]interface{}{
			"TaskStatus": common.TaskStatusRunning,
		})
		if err != nil {
			return err
		}

		compareMetas, err := meta.NewDataCompareMetaModel(r.metaDB).DetailDataCompareMeta(r.ctx, &meta.DataCompareMeta{
			DBTypeS:     r.cfg.DBTypeS,
			DBTypeT:     r.cfg.DBTypeT,
			SchemaNameS: r.cfg.OracleConfig.SchemaName,
			TableNameS:  task.sourceTableName,
			TaskMode:    r.cfg.TaskMode,
			TaskStatus:  common.TaskStatusWaiting,
		})
		if err != nil {
			return err
		}

		// 设置工作池
		// 设置 goroutine 数
		g1 := &errgroup.Group{}
		g1.SetLimit(r.cfg.DiffConfig.DiffThreads)

		for _, compareMeta := range compareMetas {
			newReport := NewReport(compareMeta, r.mysql, r.oracle, r.cfg.DiffConfig.OnlyCheckRows)
			g1.Go(func() error {
				// 数据对比报告
				report, err := IReport(newReport)
				if err != nil {
					// error skip, continue
					if err = meta.NewDataCompareMetaModel(r.metaDB).UpdateDataCompareMeta(r.ctx, &meta.DataCompareMeta{
						DBTypeS:     newReport.DataCompareMeta.DBTypeS,
						DBTypeT:     newReport.DataCompareMeta.DBTypeT,
						SchemaNameS: newReport.DataCompareMeta.SchemaNameS,
						TableNameS:  newReport.DataCompareMeta.TableNameS,
						TaskMode:    newReport.DataCompareMeta.TaskMode,
						WhereRange:  newReport.DataCompareMeta.WhereRange,
					}, map[string]interface{}{
						"TaskStatus":  common.TaskStatusFailed,
						"InfoDetail":  newReport.String(),
						"ErrorDetail": err.Error(),
					}); err != nil {
						return err
					}

					return nil
				}

				// 数据对比是否不一致
				if !strings.EqualFold(report, "") {
					var errMsg error
					errMsg = fmt.Errorf("schema table data chunk isn't euqal")

					if _, err := f.CWriteString(report); err != nil {
						errMsg = fmt.Errorf("fix sql file write failed: %v", err.Error())
					}
					// error skip, continue
					if err = meta.NewDataCompareMetaModel(r.metaDB).UpdateDataCompareMeta(r.ctx, &meta.DataCompareMeta{
						DBTypeS:     newReport.DataCompareMeta.DBTypeS,
						DBTypeT:     newReport.DataCompareMeta.DBTypeT,
						SchemaNameS: newReport.DataCompareMeta.SchemaNameS,
						TableNameS:  newReport.DataCompareMeta.TableNameS,
						TaskMode:    newReport.DataCompareMeta.TaskMode,
						WhereRange:  newReport.DataCompareMeta.WhereRange,
					}, map[string]interface{}{
						"TaskStatus":  common.TaskStatusFailed,
						"InfoDetail":  newReport.String(),
						"ErrorDetail": errMsg.Error(),
					}); err != nil {
						return err
					}

					return nil
				}

				err = meta.NewDataCompareMetaModel(r.metaDB).UpdateDataCompareMeta(r.ctx, &meta.DataCompareMeta{
					DBTypeS:     newReport.DataCompareMeta.DBTypeS,
					DBTypeT:     newReport.DataCompareMeta.DBTypeT,
					SchemaNameS: newReport.DataCompareMeta.SchemaNameS,
					TableNameS:  newReport.DataCompareMeta.TableNameS,
					TaskMode:    newReport.DataCompareMeta.TaskMode,
					WhereRange:  newReport.DataCompareMeta.WhereRange,
				}, map[string]interface{}{
					"TaskStatus": common.TaskStatusSuccess,
				})
				if err != nil {
					return err
				}
				return nil
			})
		}

		if err = g1.Wait(); err != nil {
			return fmt.Errorf("compare table task failed, update table [data_compare_meta] failed: %v", err)
		}

		// 清理元数据记录
		// 更新 wait_sync_meta 记录
		totalErrs, err := meta.NewDataCompareMetaModel(r.metaDB).CountsErrorDataCompareMeta(r.ctx, &meta.DataCompareMeta{
			DBTypeS:     r.cfg.DBTypeS,
			DBTypeT:     r.cfg.DBTypeT,
			SchemaNameS: r.cfg.OracleConfig.SchemaName,
			TableNameS:  task.sourceTableName,
			TaskMode:    r.cfg.TaskMode,
			TaskStatus:  common.TaskStatusFailed,
		})
		if err != nil {
			return fmt.Errorf("get meta table [data_compare_meta] counts failed, error: %v", err)
		}

		// 不存在错误，清理 data_compare_meta 记录, 更新 wait_sync_meta 记录
		if totalErrs == 0 {
			err = meta.NewCommonModel(r.metaDB).DeleteTableDataCompareMetaAndUpdateWaitSyncMeta(r.ctx,
				&meta.DataCompareMeta{
					DBTypeS:     r.cfg.DBTypeS,
					DBTypeT:     r.cfg.DBTypeT,
					SchemaNameS: r.cfg.OracleConfig.SchemaName,
					TableNameS:  task.sourceTableName,
					TaskMode:    r.cfg.TaskMode,
				}, &meta.WaitSyncMeta{
					DBTypeS:          r.cfg.DBTypeS,
					DBTypeT:          r.cfg.DBTypeT,
					SchemaNameS:      r.cfg.OracleConfig.SchemaName,
					TableNameS:       task.sourceTableName,
					TaskMode:         r.cfg.TaskMode,
					TaskStatus:       common.TaskStatusSuccess,
					ChunkSuccessNums: int64(len(compareMetas)),
					ChunkFailedNums:  0,
				})
			if err != nil {
				return err
			}
			zap.L().Info("diff single table oracle to mysql finished",
				zap.String("schema", r.cfg.OracleConfig.SchemaName),
				zap.String("table", task.sourceTableName),
				zap.String("cost", time.Now().Sub(diffStartTime).String()))
			// 继续
			continue
		}

		// 若存在错误，修改表状态，skip 清理，统一忽略，最后显示
		err = meta.NewWaitSyncMetaModel(r.metaDB).UpdateWaitSyncMeta(r.ctx, &meta.WaitSyncMeta{
			DBTypeS:     r.cfg.DBTypeS,
			DBTypeT:     r.cfg.DBTypeT,
			SchemaNameS: r.cfg.OracleConfig.SchemaName,
			TableNameS:  task.sourceTableName,
			TaskMode:    r.cfg.TaskMode,
		}, map[string]interface{}{
			"TaskStatus":       common.TaskStatusFailed,
			"ChunkSuccessNums": int64(len(compareMetas)) - totalErrs,
			"ChunkFailedNums":  totalErrs,
		})
		if err != nil {
			return err
		}
		zap.L().Warn("update mysql [wait_sync_meta] meta",
			zap.String("schema", r.cfg.OracleConfig.SchemaName),
			zap.String("table", task.sourceTableName),
			zap.String("mode", r.cfg.TaskMode),
			zap.String("updated", "table check exist error, skip"),
			zap.String("cost", time.Now().Sub(diffStartTime).String()))
	}
	return nil
}

func (r *O2M) compareWaitTableTasks(f *compare.File, waitTableTasks []*Task) error {
	globalSCN, err := r.oracle.GetOracleCurrentSnapshotSCN()
	if err != nil {
		return err
	}

	var chunks []*Chunk
	for cid, task := range waitTableTasks {
		sourceColumnInfo, targetColumnInfo, err := task.AdjustDBSelectColumn()
		if err != nil {
			return err
		}
		whereColumn, err := task.FilterDBWhereColumn()
		if err != nil {
			return err
		}
		isPartition, err := task.IsPartitionTable()
		if err != nil {
			return err
		}
		chunks = append(chunks, NewChunk(r.ctx, r.cfg, r.oracle, r.mysql, r.metaDB,
			cid, globalSCN, task.sourceTableName, task.targetTableName, isPartition, sourceColumnInfo, targetColumnInfo,
			whereColumn))
	}

	// chunk split
	g := &errgroup.Group{}
	g.SetLimit(r.cfg.DiffConfig.DiffThreads)
	for _, chunk := range chunks {
		c := chunk
		g.Go(func() error {
			err := IChunker(c)
			if err != nil {
				return err
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	err = r.comparePartTableTasks(f, waitTableTasks)
	if err != nil {
		return err
	}
	return nil
}
