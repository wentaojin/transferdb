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
	"github.com/wentaojin/transferdb/module/query/mysql"
	"github.com/wentaojin/transferdb/module/query/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strconv"
	"strings"
	"time"
)

type O2M struct {
	ctx    context.Context
	cfg    *config.Config
	oracle *oracle.Oracle
	mysql  *mysql.MySQL
}

func NewO2MFuller(ctx context.Context, cfg *config.Config, oracle *oracle.Oracle, mysql *mysql.MySQL) *O2M {
	return &O2M{
		ctx:    ctx,
		cfg:    cfg,
		oracle: oracle,
		mysql:  mysql,
	}
}

func (r *O2M) NewFuller() error {
	startTime := time.Now()
	zap.L().Info("source schema full table data sync start",
		zap.String("schema", r.cfg.OracleConfig.SchemaName))

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oracleDBVersion, err := r.oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if common.VersionOrdinal(oracleDBVersion) < common.VersionOrdinal(common.OracleSYNCRequireDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using transferdb tools", oracleDBVersion)
	}
	oracleCollation := false
	if common.VersionOrdinal(oracleDBVersion) >= common.VersionOrdinal(common.OracleTableColumnCollationDBVersion) {
		oracleCollation = true
	}

	// 获取配置文件待同步表列表
	exporters, err := filterCFGTable(r.cfg, r.oracle)
	if err != nil {
		return err
	}

	// 判断 table_error_detail 是否存在错误记录，是否可进行 CSV
	errTotals, err := model.NewTableErrorDetailModel(r.oracle.GormDB).CountsBySchema(r.ctx, &model.TableErrorDetail{
		SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
		RunMode:          common.FullO2MMode,
	})
	if errTotals > 0 || err != nil {
		return fmt.Errorf("full schema [%s] table mode [%s] task failed: %v, table [table_error_detail] exist failed error, please clear and rerunning", strings.ToUpper(r.cfg.OracleConfig.SchemaName), common.CompareO2MMode, err)
	}

	// 判断并记录待同步表列表
	for _, tableName := range exporters {
		waitSyncMetas, err := model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Detail(r.ctx, &model.WaitSyncMeta{
			SourceSchemaName: r.cfg.OracleConfig.SchemaName,
			SourceTableName:  tableName,
			SyncMode:         common.FullO2MMode,
		})
		if err != nil {
			return err
		}
		// 初始同步表全量任务为 -1 表示未进行全量初始化，初始化完成会变更
		// 全量同步完成，增量阶段，值预期都是 0
		if len(waitSyncMetas.([]model.WaitSyncMeta)) == 0 {
			// 初始同步表全量任务为 -1 表示未进行全量初始化，初始化完成会变更
			// 全量同步完成，增量阶段，值预期都是 0
			err = model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Create(r.ctx, &model.WaitSyncMeta{
				SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
				SourceTableName:  common.StringUPPER(tableName),
				SyncMode:         common.FullO2MMode,
				FullGlobalSCN:    0,
				FullSplitTimes:   -1,
			})
			if err != nil {
				return err
			}
		}
	}

	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,清理元数据表 [wait_sync_meta],重新运行全量任务
	if !r.cfg.FullConfig.EnableCheckpoint {
		err = model.NewSyncMetaModel(r.oracle.GormDB).FullSyncMeta.DeleteBySchemaSyncMode(r.ctx, &model.FullSyncMeta{
			SourceSchemaName: r.cfg.OracleConfig.SchemaName,
			SyncMode:         common.FullO2MMode,
		})
		if err != nil {
			return err
		}
		for _, tableName := range exporters {
			err = model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Delete(r.ctx, &model.WaitSyncMeta{
				SourceSchemaName: r.cfg.OracleConfig.SchemaName,
				SourceTableName:  tableName,
				SyncMode:         common.FullO2MMode,
			})
			if err != nil {
				return err
			}
			// 清理已有表数据
			if err := r.mysql.TruncateMySQLTable(r.cfg.MySQLConfig.SchemaName, tableName); err != nil {
				return err
			}
			// 判断并记录待同步表列表
			waitSyncMetas, err := model.NewSyncMetaModel(r.oracle.GormDB).WaitSyncMeta.Detail(r.ctx, &model.WaitSyncMeta{
				SourceSchemaName: r.cfg.OracleConfig.SchemaName,
				SourceTableName:  tableName,
				SyncMode:         common.FullO2MMode,
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
					SyncMode:         common.FullO2MMode,
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
		SyncMode:         common.FullO2MMode,
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
		SyncMode:         common.FullO2MMode,
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
		zap.L().Info("source schema full table data finished",
			zap.String("schema", r.cfg.OracleConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()))
		return nil
	}

	// 判断未同步完成的表列表能否断点续传
	var panicTblFullSlice []string

	for _, partSyncMeta := range partSyncTableMetas {
		tableNameArray, err2 := model.NewSyncMetaModel(r.oracle.GormDB).FullSyncMeta.DistinctTableName(r.ctx, &model.FullSyncMeta{
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
		zap.L().Error("source schema full table data sync error",
			zap.String("schema", r.cfg.OracleConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Strings("panic tables", panicTblFullSlice))
		return fmt.Errorf("checkpoint isn't consistent, please reruning [enable-checkpoint = fase]")
	}

	// 数据迁移
	// 优先存在断点的表
	// partSyncTables -> waitSyncTables
	if len(partSyncTables) > 0 {
		err = r.fullPartSyncTable(partSyncTables)
		if err != nil {
			return err
		}
	}
	if len(waitSyncTables) > 0 {
		err = r.fullWaitSyncTable(waitSyncTables, oracleCollation)
		if err != nil {
			return err
		}
	}

	endTime := time.Now()
	zap.L().Info("all full table data sync finished",
		zap.String("schema", r.cfg.OracleConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func (r *O2M) fullPartSyncTable(fullPartTables []string) error {
	taskTime := time.Now()

	g := &errgroup.Group{}
	g.SetLimit(r.cfg.FullConfig.TableThreads)

	for _, table := range fullPartTables {
		t := table
		g.Go(func() error {
			startTime := time.Now()
			fullMetas, err := model.NewSyncMetaModel(r.mysql.GormDB).FullSyncMeta.Detail(r.ctx, &model.FullSyncMeta{
				SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
				SourceTableName:  common.StringUPPER(t),
				SyncMode:         common.FullO2MMode,
			})
			if err != nil {
				return err
			}

			g1 := &errgroup.Group{}
			g1.SetLimit(r.cfg.FullConfig.SQLThreads)
			for _, meta := range fullMetas.([]model.FullSyncMeta) {
				m := meta
				g.Go(func() error {
					// 数据写入
					columnFields, batchResults, err := IExtractor(
						NewTable(r.ctx, m, r.oracle, r.cfg.AppConfig.InsertBatchSize))
					if err != nil {
						return err
					}
					err = ITranslator(NewChunk(r.ctx, m, r.oracle, r.mysql, columnFields, batchResults, r.cfg.FullConfig.ApplyThreads, r.cfg.AppConfig.InsertBatchSize, true))
					if err != nil {
						return err
					}
					err = IApplier(NewChunk(r.ctx, m, r.oracle, r.mysql, columnFields, batchResults, r.cfg.FullConfig.ApplyThreads, r.cfg.AppConfig.InsertBatchSize, true))
					if err != nil {
						return err
					}
					return nil
				})
			}

			if err = g1.Wait(); err != nil {
				return err
			}

			// 更新表级别记录
			err = model.NewSyncMetaModel(r.mysql.GormDB).WaitSyncMeta.ModifyFullSplitTimesZero(r.ctx, &model.WaitSyncMeta{
				SourceSchemaName: r.cfg.OracleConfig.SchemaName,
				SourceTableName:  t,
				SyncMode:         common.FullO2MMode,
				FullSplitTimes:   0,
			})
			if err != nil {
				return err
			}

			zap.L().Info("source schema table data finished",
				zap.String("schema", r.cfg.OracleConfig.SchemaName),
				zap.String("table", t),
				zap.String("cost", time.Now().Sub(startTime).String()))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	zap.L().Info("source schema all table data loader finished",
		zap.String("schema", r.cfg.OracleConfig.SchemaName),
		zap.String("cost", time.Now().Sub(taskTime).String()))
	return nil
}

func (r *O2M) fullWaitSyncTable(fullWaitTables []string, oracleCollation bool) error {
	err := r.initWaitSyncTableRowID(fullWaitTables, oracleCollation)
	if err != nil {
		return err
	}
	err = r.fullPartSyncTable(fullWaitTables)
	if err != nil {
		return err
	}

	return nil
}

func (r *O2M) initWaitSyncTableRowID(csvWaitTables []string, oracleCollation bool) error {
	startTask := time.Now()
	// 全量同步前，获取 SCN 以及初始化元数据表
	globalSCN, err := r.oracle.GetOracleCurrentSnapshotSCN()
	if err != nil {
		return err
	}
	partitionTables, err := r.oracle.GetOracleSchemaPartitionTable(r.cfg.OracleConfig.SchemaName)
	if err != nil {
		return err
	}

	g := &errgroup.Group{}
	g.SetLimit(r.cfg.CSVConfig.TaskThreads)

	for idx, table := range csvWaitTables {
		t := table
		workerID := idx
		g.Go(func() error {
			startTime := time.Now()

			sourceColumnInfo, err := r.adjustTableSelectColumn(t, oracleCollation)
			if err != nil {
				return err
			}

			var (
				isPartition string
			)
			if common.IsContainString(partitionTables, common.StringUPPER(t)) {
				isPartition = "YES"
			} else {
				isPartition = "NO"
			}

			tableRowsByStatistics, err := r.oracle.GetOracleTableRowsByStatistics(r.cfg.OracleConfig.SchemaName, t)
			if err != nil {
				return err
			}
			// 统计信息数据行数 0，直接全表扫
			if tableRowsByStatistics == 0 {
				zap.L().Warn("get oracle table rows",
					zap.String("schema", r.cfg.OracleConfig.SchemaName),
					zap.String("table", t),
					zap.String("column", sourceColumnInfo),
					zap.String("where", "1 = 1"),
					zap.Int("statistics rows", tableRowsByStatistics))

				err = model.NewCommonModel(r.oracle.GormDB).CreateFullSyncMetaAndUpdateWaitSyncMeta(r.ctx, &model.FullSyncMeta{
					SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
					SourceTableName:  common.StringUPPER(t),
					GlobalSCN:        globalSCN,
					SourceColumnInfo: sourceColumnInfo,
					SourceRowidInfo:  "1 = 1",
					SyncMode:         common.FullO2MMode,
					IsPartition:      isPartition,
				}, &model.WaitSyncMeta{
					SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
					SourceTableName:  common.StringUPPER(t),
					SyncMode:         common.FullO2MMode,
					FullGlobalSCN:    globalSCN,
					FullSplitTimes:   1,
					IsPartition:      isPartition,
				})
				if err != nil {
					return err
				}
				return nil
			}

			zap.L().Info("get oracle table statistics rows",
				zap.String("schema", common.StringUPPER(r.cfg.OracleConfig.SchemaName)),
				zap.String("table", common.StringUPPER(t)),
				zap.Int("rows", tableRowsByStatistics))

			taskName := common.StringsBuilder(common.StringUPPER(r.cfg.OracleConfig.SchemaName), `_`, common.StringUPPER(t), `_`, `TASK`, strconv.Itoa(workerID))

			if err = r.oracle.StartOracleChunkCreateTask(taskName); err != nil {
				return err
			}

			if err = r.oracle.StartOracleCreateChunkByRowID(taskName, common.StringUPPER(r.cfg.OracleConfig.SchemaName), common.StringUPPER(t), strconv.Itoa(r.cfg.CSVConfig.Rows)); err != nil {
				return err
			}

			chunkRes, err := r.oracle.GetOracleTableChunksByRowID(taskName)
			if err != nil {
				return err
			}

			// 判断数据是否存在
			if len(chunkRes) == 0 {
				zap.L().Warn("get oracle table rowids rows",
					zap.String("schema", common.StringUPPER(r.cfg.OracleConfig.SchemaName)),
					zap.String("table", common.StringUPPER(t)),
					zap.String("column", sourceColumnInfo),
					zap.String("where", "1 = 1"),
					zap.Int("rowids rows", len(chunkRes)))

				err = model.NewCommonModel(r.oracle.GormDB).CreateFullSyncMetaAndUpdateWaitSyncMeta(r.ctx, &model.FullSyncMeta{
					SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
					SourceTableName:  common.StringUPPER(t),
					GlobalSCN:        globalSCN,
					SourceColumnInfo: sourceColumnInfo,
					SourceRowidInfo:  "1 = 1",
					SyncMode:         common.FullO2MMode,
					IsPartition:      isPartition,
				}, &model.WaitSyncMeta{
					SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
					SourceTableName:  common.StringUPPER(t),
					SyncMode:         common.FullO2MMode,
					FullGlobalSCN:    globalSCN,
					FullSplitTimes:   1,
					IsPartition:      isPartition,
				})
				if err != nil {
					return err
				}

				return nil
			}

			var fullMetas []model.FullSyncMeta
			for _, res := range chunkRes {
				fullMetas = append(fullMetas, model.FullSyncMeta{
					SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
					SourceTableName:  common.StringUPPER(t),
					GlobalSCN:        globalSCN,
					SourceColumnInfo: sourceColumnInfo,
					SourceRowidInfo:  res["CMD"],
					SyncMode:         common.FullO2MMode,
					IsPartition:      isPartition,
				})
			}

			// 元数据库信息 batch 写入
			err = model.NewSyncMetaModel(r.mysql.GormDB).FullSyncMeta.BatchCreate(r.ctx, fullMetas, r.cfg.AppConfig.InsertBatchSize)
			if err != nil {
				return err
			}

			// 更新 wait_sync_meta
			err = model.NewSyncMetaModel(r.mysql.GormDB).WaitSyncMeta.Update(r.ctx, &model.WaitSyncMeta{
				SourceSchemaName: common.StringUPPER(r.cfg.OracleConfig.SchemaName),
				SourceTableName:  common.StringUPPER(t),
				SyncMode:         common.FullO2MMode,
				FullGlobalSCN:    globalSCN,
				FullSplitTimes:   len(chunkRes),
				IsPartition:      isPartition,
			})
			if err != nil {
				return err
			}

			if err = r.oracle.CloseOracleChunkTask(taskName); err != nil {
				return err
			}

			endTime := time.Now()
			zap.L().Info("source table init wait_sync_meta and full_sync_meta finished",
				zap.String("schema", r.cfg.OracleConfig.SchemaName),
				zap.String("table", t),
				zap.String("cost", endTime.Sub(startTime).String()))
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return err
	}

	zap.L().Info("source schema init wait_sync_meta and full_sync_meta finished",
		zap.String("schema", r.cfg.OracleConfig.SchemaName),
		zap.String("cost", time.Now().Sub(startTask).String()))
	return nil
}

func (r *O2M) adjustTableSelectColumn(sourceTable string, oracleCollation bool) (string, error) {
	// Date/Timestamp 字段类型格式化
	// Interval Year/Day 数据字符 TO_CHAR 格式化
	columnsINFO, err := r.oracle.GetOracleSchemaTableColumn(r.cfg.OracleConfig.SchemaName, sourceTable, oracleCollation)
	if err != nil {
		return "", err
	}

	var columnNames []string

	for _, rowCol := range columnsINFO {
		switch strings.ToUpper(rowCol["DATA_TYPE"]) {
		// 数字
		case "NUMBER":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
		case "DECIMAL", "DEC", "DOUBLE PRECISION", "FLOAT", "INTEGER", "INT", "REAL", "NUMERIC", "BINARY_FLOAT", "BINARY_DOUBLE", "SMALLINT":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
		// 字符
		case "BFILE", "CHARACTER", "LONG", "NCHAR VARYING", "ROWID", "UROWID", "VARCHAR", "XMLTYPE", "CHAR", "NCHAR", "NVARCHAR2", "NCLOB", "CLOB":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
		// 二进制
		case "BLOB", "LONG RAW", "RAW":
			columnNames = append(columnNames, rowCol["COLUMN_NAME"])
		// 时间
		case "DATE":
			columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ",'yyyy-MM-dd HH24:mi:ss') AS ", rowCol["COLUMN_NAME"]))
		// 默认其他类型
		default:
			if strings.Contains(rowCol["DATA_TYPE"], "INTERVAL") {
				columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ") AS ", rowCol["COLUMN_NAME"]))
			} else if strings.Contains(rowCol["DATA_TYPE"], "TIMESTAMP") {
				dataScale, err := strconv.Atoi(rowCol["DATA_SCALE"])
				if err != nil {
					return "", fmt.Errorf("aujust oracle timestamp datatype scale [%s] strconv.Atoi failed: %v", rowCol["DATA_SCALE"], err)
				}
				if dataScale == 0 {
					columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ",'yyyy-mm-dd hh24:mi:ss') AS ", rowCol["COLUMN_NAME"]))
				} else if dataScale < 0 && dataScale <= 6 {
					columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"],
						",'yyyy-mm-dd hh24:mi:ss.ff", rowCol["DATA_SCALE"], "') AS ", rowCol["COLUMN_NAME"]))
				} else {
					columnNames = append(columnNames, common.StringsBuilder("TO_CHAR(", rowCol["COLUMN_NAME"], ",'yyyy-mm-dd hh24:mi:ss.ff6') AS ", rowCol["COLUMN_NAME"]))
				}

			} else {
				columnNames = append(columnNames, rowCol["COLUMN_NAME"])
			}
		}

	}

	return strings.Join(columnNames, ","), nil
}
