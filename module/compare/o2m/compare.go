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
	"github.com/wentaojin/transferdb/model"
	"github.com/wentaojin/transferdb/module/compare"
	"github.com/wentaojin/transferdb/module/query/mysql"
	"github.com/wentaojin/transferdb/module/query/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type O2M struct {
	ctx    context.Context
	cfg    *config.Config
	oracle *oracle.Oracle
	mysql  *mysql.MySQL
}

func NewO2MCompare(ctx context.Context, cfg *config.Config, oracle *oracle.Oracle, mysql *mysql.MySQL) *O2M {
	return &O2M{
		ctx:    ctx,
		cfg:    cfg,
		oracle: oracle,
		mysql:  mysql,
	}
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
	if common.VersionOrdinal(oraDBVersion) < common.VersionOrdinal(common.OracleSYNCRequireDBVersion) {
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

	// 判断 table_error_detail 是否存在错误记录，是否可进行 compare
	errTotals, err := model.NewTableErrorDetailModel(r.oracle.GormDB).CountsBySchema(r.ctx, &model.TableErrorDetail{
		SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		RunMode:          common.CompareO2MMode,
	})
	if errTotals > 0 || err != nil {
		return fmt.Errorf("compare schema [%s] table mode [%s] task failed: %v, table [table_error_detail] exist failed error, please clear and rerunning", strings.ToUpper(r.cfg.OracleConfig.SchemaName), common.CompareO2MMode, err)
	}

	// 判断并记录待同步表列表
	for _, tableName := range exporters {
		waitSyncMetas, err := model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Detail(r.ctx, &model.WaitSyncMeta{
			SourceSchemaName: r.cfg.OracleConfig.SchemaName,
			SourceTableName:  tableName,
			SyncMode:         common.CompareO2MMode,
		})
		if err != nil {
			return err
		}
		if len(waitSyncMetas.([]model.WaitSyncMeta)) == 0 {
			// 初始同步表全量任务为 -1 表示未进行全量初始化，初始化完成会变更
			// 全量同步完成，增量阶段，值预期都是 0
			err = model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Create(r.ctx, &model.WaitSyncMeta{
				SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
				SourceTableName:  common.StringUPPER(tableName),
				SyncMode:         common.CompareO2MMode,
				FullGlobalSCN:    0,
				FullSplitTimes:   -1,
			})
			if err != nil {
				return err
			}
		}
	}

	// 关于全量断点恢复
	if !r.cfg.DiffConfig.EnableCheckpoint {
		err = model.NewDataCompareMetaModel(r.oracle.GormDB).Truncate(r.ctx, &model.DataCompareMeta{})
		if err != nil {
			return err
		}

		for _, tableName := range exporters {
			err = model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Delete(r.ctx, &model.WaitSyncMeta{
				SourceSchemaName: r.cfg.OracleConfig.SchemaName,
				SourceTableName:  tableName,
				SyncMode:         common.CompareO2MMode,
			})
			if err != nil {
				return err
			}

			// 判断并记录待同步表列表
			waitSyncMetas, err := model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Detail(r.ctx, &model.WaitSyncMeta{
				SourceSchemaName: r.cfg.OracleConfig.SchemaName,
				SourceTableName:  tableName,
				SyncMode:         common.CompareO2MMode,
			})
			if err != nil {
				return err
			}
			if len(waitSyncMetas.([]model.WaitSyncMeta)) == 0 {
				// 初始同步表全量任务为 -1 表示未进行全量初始化，初始化完成会变更
				// 全量同步完成，增量阶段，值预期都是 0
				err = model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Create(r.ctx, &model.WaitSyncMeta{
					SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
					SourceTableName:  common.StringUPPER(tableName),
					SyncMode:         common.CompareO2MMode,
					FullGlobalSCN:    0,
					FullSplitTimes:   -1,
				})
				if err != nil {
					return err
				}
			}
		}
	}

	// 获取等待同步以及未同步完成的表列表
	var (
		waitSyncTableMetas []model.WaitSyncMeta
		waitSyncTables     []string
	)

	waitSyncDetails, err := model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Detail(r.ctx, &model.WaitSyncMeta{
		SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		SyncMode:         common.CompareO2MMode,
		FullGlobalSCN:    0,
		FullSplitTimes:   -1,
	})
	if err != nil {
		return err
	}
	waitSyncTableMetas = waitSyncDetails.([]model.WaitSyncMeta)
	if len(waitSyncTableMetas) > 0 {
		for _, table := range waitSyncTableMetas {
			waitSyncTables = append(waitSyncTables, common.StringUPPER(table.SourceTableName))
		}
	}

	var (
		partSyncTableMetas []model.WaitSyncMeta
		partSyncTables     []string
	)
	partSyncDetails, err := model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Query(r.ctx, &model.WaitSyncMeta{
		SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		SyncMode:         common.CompareO2MMode,
	})
	if err != nil {
		return err
	}
	partSyncTableMetas = partSyncDetails.([]model.WaitSyncMeta)
	if len(partSyncTableMetas) > 0 {
		for _, table := range partSyncTableMetas {
			partSyncTables = append(partSyncTables, common.StringUPPER(table.SourceTableName))
		}
	}

	if len(waitSyncTableMetas) == 0 && len(partSyncTableMetas) == 0 {
		endTime := time.Now()
		zap.L().Info("all oracle table data diff finished",
			zap.String("schema", r.cfg.OracleConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()))
		return nil
	}

	// 判断未同步完成的表列表能否断点续传
	var panicTblFullSlice []string

	for _, partSyncMeta := range partSyncTableMetas {
		tableNameArray, err2 := model.NewDataCompareMetaModel(r.oracle.GormDB).DistinctTableName(r.ctx, &model.DataCompareMeta{
			SourceSchemaName: r.cfg.OracleConfig.SchemaName})
		if err2 != nil {
			return err2
		}

		if !common.IsContainString(tableNameArray.([]string), partSyncMeta.SourceTableName) {
			panicTblFullSlice = append(panicTblFullSlice, partSyncMeta.SourceTableName)
		}
	}

	if len(panicTblFullSlice) != 0 {
		endTime := time.Now()
		zap.L().Error("all oracle table data diff error",
			zap.String("schema", r.cfg.OracleConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Strings("panic tables", panicTblFullSlice))
		return fmt.Errorf("checkpoint isn't consistent, please reruning [enable-checkpoint = fase]")
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

	var (
		oracleTableCollation  map[string]string
		oracleSchemaCollation string
	)

	if oracleCollation {
		beginTime = time.Now()
		oracleSchemaCollation, err = r.oracle.GetOracleSchemaCollation(common.StringUPPER(r.cfg.OracleConfig.SchemaName))
		if err != nil {
			return err
		}
		oracleTableCollation, err = r.oracle.GetOracleSchemaTableCollation(common.StringUPPER(r.cfg.OracleConfig.SchemaName), oracleSchemaCollation)
		if err != nil {
			return err
		}
		finishTime = time.Now()
		zap.L().Info("get oracle schema and table collation finished",
			zap.String("schema", r.cfg.OracleConfig.SchemaName),
			zap.String("db version", oraDBVersion),
			zap.String("db character", oracleDBCharacterSet),
			zap.Int("table totals", len(exporters)),
			zap.Bool("table collation", oracleCollation),
			zap.String("cost", finishTime.Sub(beginTime).String()))
	}

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
	partTableTasks := NewPartCompareTableTask(r.ctx, r.cfg, partSyncTables, r.mysql, r.oracle)
	waitTableTasks := NewWaitCompareTableTask(r.ctx, r.cfg, waitSyncTables, oracleDBCharacterSet, nlsComp, oracleTableCollation, oracleSchemaCollation, oracleCollation, r.mysql, r.oracle)

	// 数据对比
	pwdDir, err := os.Getwd()
	if err != nil {
		return err
	}

	checkFile := filepath.Join(pwdDir, r.cfg.DiffConfig.FixSqlFile)

	// file writer
	f, err := compare.NewWriter(checkFile)
	if err != nil {
		return err
	}

	// 优先存在断点的表校验
	// partTableTask -> waitTableTasks
	if len(partTableTasks) > 0 {
		err = r.comparePartTableTasks(f, partTableTasks)
		if err != nil {
			return err
		}
	}
	if len(waitTableTasks) > 0 {
		err = r.compareWaitTableTasks(f, waitTableTasks)
		if err != nil {
			return err
		}
	}
	// 错误核对
	errTotals, err = model.NewTableErrorDetailModel(r.oracle.GormDB).CountsBySchema(r.ctx, &model.TableErrorDetail{
		SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		RunMode:          common.CompareO2MMode,
	})
	if err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("diff", zap.String("fix sql file output", filepath.Join(pwdDir, r.cfg.DiffConfig.FixSqlFile)))
	if errTotals == 0 {
		zap.L().Info("diff table oracle to mysql finished",
			zap.Int("table totals", len(exporters)),
			zap.Int("table success", len(exporters)),
			zap.Int("table failed", int(errTotals)),
			zap.String("cost", endTime.Sub(startTime).String()))
	} else {
		zap.L().Warn("diff table oracle to mysql finished",
			zap.Int("table totals", len(exporters)),
			zap.Int("table success", len(exporters)-int(errTotals)),
			zap.Int("table failed", int(errTotals)),
			zap.String("failed tips", "failed detail, please see table [table_error_detail]"),
			zap.String("cost", endTime.Sub(startTime).String()))
	}
	return nil
}

func (r *O2M) comparePartTableTasks(f *compare.File, partTableTasks []*Task) error {
	for _, task := range partTableTasks {
		err := IProcessor(task)
		if err != nil {
			return err
		}

		// 获取对比记录
		diffStartTime := time.Now()
		compareMetas, err := model.NewDataCompareMetaModel(r.mysql.GormDB).Detail(r.ctx, &model.DataCompareMeta{
			SourceSchemaName: r.cfg.OracleConfig.SchemaName,
			SourceTableName:  task.tableName,
			TargetSchemaName: r.cfg.MySQLConfig.SchemaName,
			TargetTableName:  task.tableName,
		})
		if err != nil {
			return err
		}

		// 设置工作池
		// 设置 goroutine 数
		g := &errgroup.Group{}
		g.SetLimit(r.cfg.DiffConfig.DiffThreads)

		for _, compareMeta := range compareMetas.([]model.DataCompareMeta) {
			newReport := NewReport(compareMeta, r.mysql, r.oracle, r.cfg.DiffConfig.OnlyCheckRows)
			g.Go(func() error {
				// 数据对比报告
				if err = IReport(newReport, f); err != nil {
					err := model.NewTableErrorDetailModel(r.mysql.GormDB).Create(r.ctx, &model.TableErrorDetail{
						SourceSchemaName: newReport.dataCompareMeta.SourceSchemaName,
						SourceTableName:  newReport.dataCompareMeta.SourceTableName,
						RunMode:          common.CompareO2MMode,
						InfoSources:      common.CompareO2MMode,
						RunStatus:        "Failed",
						InfoDetail:       newReport.String(),
						ErrorDetail:      fmt.Sprintf("data diff record report failed: %v", err.Error()),
					})
					if err != nil {
						return fmt.Errorf("write meta table [table_error_detail] records failed, error: %v", err)
					}
					// continue
				}

				// 清理元数据记录
				errCounts, err := model.NewTableErrorDetailModel(r.mysql.GormDB).Counts(r.ctx, &model.TableErrorDetail{
					SourceSchemaName: newReport.dataCompareMeta.SourceSchemaName,
					SourceTableName:  newReport.dataCompareMeta.SourceTableName,
					RunMode:          common.CompareO2MMode,
				})
				if err != nil {
					return fmt.Errorf("get meta table [table_error_detail] counts failed, error: %v", err)
				}
				// 若存在错误，skip 清理
				if errCounts >= 1 {
					return nil
				}

				err = model.NewDataCompareMetaModel(r.mysql.GormDB).Delete(r.ctx, &model.DataCompareMeta{
					SourceSchemaName: newReport.dataCompareMeta.SourceSchemaName,
					SourceTableName:  newReport.dataCompareMeta.SourceTableName,
					WhereRange:       newReport.dataCompareMeta.WhereRange,
				})
				if err != nil {
					return err
				}
				zap.L().Info("delete mysql [data_diff_meta] meta",
					zap.String("table", newReport.String()),
					zap.String("status", "success"))
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			zap.L().Error("diff table oracle to mysql failed",
				zap.String("schema", r.cfg.OracleConfig.SchemaName),
				zap.String("table", task.tableName),
				zap.Error(fmt.Errorf("diff table task failed, detail see [table_error_detail], please rerunning")))
			// 忽略错误 continue
			continue
		}

		// 更新 wait_sync_meta 记录
		errCounts, err := model.NewTableErrorDetailModel(r.mysql.GormDB).Counts(r.ctx, &model.TableErrorDetail{
			SourceSchemaName: r.cfg.OracleConfig.SchemaName,
			SourceTableName:  task.tableName,
			RunMode:          common.CompareO2MMode,
		})
		if err != nil {
			return fmt.Errorf("get meta table [table_error_detail] counts failed, error: %v", err)
		}
		// 若存在错误，skip 清理，统一忽略，最后显示
		if errCounts >= 1 {
			zap.L().Warn("update mysql [wait_sync_meta] meta",
				zap.String("schema", r.cfg.OracleConfig.SchemaName),
				zap.String("table", task.tableName),
				zap.String("mode", common.CompareO2MMode),
				zap.String("updated", "skip"))
			return nil
		}

		err = model.NewSyncMetaModel(r.mysql.GormDB).WaitSyncMeta.ModifyFullSplitTimesZero(r.ctx, &model.WaitSyncMeta{
			SourceSchemaName: r.cfg.OracleConfig.SchemaName,
			SourceTableName:  task.tableName,
			SyncMode:         common.CompareO2MMode,
			FullSplitTimes:   0,
		})
		if err != nil {
			return err
		}
		diffEndTime := time.Now()
		zap.L().Info("diff single table oracle to mysql finished",
			zap.String("schema", r.cfg.OracleConfig.SchemaName),
			zap.String("table", task.tableName),
			zap.String("cost", diffEndTime.Sub(diffStartTime).String()))
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
		err := task.PreTableStructCheck()
		if err != nil {
			return err
		}
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
		chunks = append(chunks, NewChunk(r.ctx, r.cfg, r.oracle, r.mysql,
			cid, globalSCN, r.cfg.OracleConfig.SchemaName, task.tableName, isPartition, sourceColumnInfo, targetColumnInfo,
			whereColumn, common.CompareO2MMode))
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
