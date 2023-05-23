/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or impliec.
See the License for the specific language governing permissions and
limitations under the License.
*/
package o2t

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

// Chunk 数据对比
type Chunk struct {
	Ctx              context.Context `json:"-"`
	ChunkID          int             `json:"chunk_id"`
	SourceGlobalSCN  uint64          `json:"source_global_scn"`
	SourceTable      string          `json:"source_table"`
	TargetTable      string          `json:"target_table"`
	IsPartition      string          `json:"is_partition"`
	SourceColumnInfo string          `json:"source_column_info"`
	TargetColumnInfo string          `json:"target_column_info"`
	WhereColumn      string          `json:"where_column"`
	WhereRange       string          `json:"where_range"` // chunk split need
	Cfg              *config.Config  `json:"-"`
	Oracle           *oracle.Oracle  `json:"-"`
	MySQL            *mysql.MySQL    `json:"-"`
	MetaDB           *meta.Meta      `json:"-"`
}

func NewChunk(ctx context.Context, cfg *config.Config, oracle *oracle.Oracle, mysql *mysql.MySQL, metaDB *meta.Meta,
	chunkID int, sourceGlobalSCN uint64, sourceTable, targetTable string, isPartition string, sourceColumnInfo, targetColumnInfo string,
	whereColumn string) *Chunk {
	return &Chunk{
		Ctx:              ctx,
		ChunkID:          chunkID,
		SourceGlobalSCN:  sourceGlobalSCN,
		SourceTable:      sourceTable,
		TargetTable:      targetTable,
		IsPartition:      isPartition,
		SourceColumnInfo: sourceColumnInfo,
		TargetColumnInfo: targetColumnInfo,
		WhereColumn:      whereColumn,
		Oracle:           oracle,
		MySQL:            mysql,
		MetaDB:           metaDB,
		Cfg:              cfg,
	}
}

func (c *Chunk) CustomTableConfig() (customColumn string, customRange string, err error) {
	// 获取配置文件自定义配置
	for _, tableCfg := range c.Cfg.DiffConfig.TableConfig {
		if strings.EqualFold(c.SourceTable, tableCfg.SourceTable) {
			// 同张表 indexFields vs Range 优先级，indexFields 需要是 number 数据类型字段
			// 同张表如果同时存在 indexFields 以及 Range，那么 Range 优先级 > indexFields
			if tableCfg.IndexFields != "" && tableCfg.Range == "" {
				isNUMBER, err := c.Oracle.IsNumberColumnTYPE(c.Cfg.OracleConfig.SchemaName, tableCfg.SourceTable, tableCfg.IndexFields)
				if err != nil || !isNUMBER {
					zap.L().Warn("compare table config index filed isn't number data type",
						zap.String("table", tableCfg.SourceTable),
						zap.String("index filed", tableCfg.IndexFields),
						zap.String("range", tableCfg.Range))
					return customColumn, customRange, fmt.Errorf("config file index-filed isn't number type, error: %v", err)
				}
				customColumn = tableCfg.IndexFields
				return customColumn, customRange, nil
			}

			if tableCfg.IndexFields == "" && tableCfg.Range != "" {
				customRange = tableCfg.Range
				return customColumn, customRange, nil
			}

			if tableCfg.IndexFields == "" && tableCfg.Range == "" {
				return customColumn, customRange, nil
			}

			if tableCfg.IndexFields != "" && tableCfg.Range != "" {
				customRange = tableCfg.Range
				return customColumn, customRange, nil
			}
		}
	}
	return customColumn, customRange, nil
}

func (c *Chunk) Split() error {
	startTime := time.Now()

	// 配置文件参数优先级
	// onlyCheckRows > configRange > configIndexFiled > DBFilter Integer Column
	// first
	if c.Cfg.DiffConfig.OnlyCheckRows {
		// SELECT COUNT(1) FROM TAB WHERE 1=1
		c.SourceColumnInfo = "COUNT(1)"
		c.TargetColumnInfo = "COUNT(1)"
		c.WhereColumn = ""
		c.WhereRange = "1 = 1"

		err := meta.NewCommonModel(c.MetaDB).CreateDataCompareMetaAndUpdateWaitSyncMeta(c.Ctx, &meta.DataCompareMeta{
			DBTypeS:       c.Cfg.DBTypeS,
			DBTypeT:       c.Cfg.DBTypeT,
			SchemaNameS:   common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:    common.StringUPPER(c.SourceTable),
			ColumnDetailS: c.SourceColumnInfo,
			SchemaNameT:   common.StringUPPER(c.Cfg.MySQLConfig.SchemaName),
			TableNameT:    common.StringUPPER(c.TargetTable),
			ColumnDetailT: c.TargetColumnInfo,
			WhereColumn:   c.WhereColumn,
			WhereRange:    c.WhereRange,
			TaskMode:      c.Cfg.TaskMode,
			TaskStatus:    common.TaskStatusWaiting,
			IsPartition:   c.IsPartition,
		}, &meta.WaitSyncMeta{
			DBTypeS:          c.Cfg.DBTypeS,
			DBTypeT:          c.Cfg.DBTypeT,
			SchemaNameS:      common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:       common.StringUPPER(c.SourceTable),
			TaskMode:         c.Cfg.TaskMode,
			GlobalScnS:       c.SourceGlobalSCN,
			ChunkTotalNums:   1,
			ChunkSuccessNums: 0,
			ChunkFailedNums:  0,
			IsPartition:      c.IsPartition,
		})
		if err != nil {
			return err
		}

		return nil
	}

	// second
	// Range > IndexFields
	customColumn, customRange, err := c.CustomTableConfig()
	if err != nil {
		return err
	}

	if !strings.EqualFold(customRange, "") {
		// range = "age > 1 and age < 10"
		// select xxx from tab where age > 1 and age < 10
		c.WhereRange = customRange
		c.WhereColumn = ""
		err = meta.NewCommonModel(c.MetaDB).CreateDataCompareMetaAndUpdateWaitSyncMeta(c.Ctx, &meta.DataCompareMeta{
			DBTypeS:       c.Cfg.DBTypeS,
			DBTypeT:       c.Cfg.DBTypeT,
			SchemaNameS:   common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:    common.StringUPPER(c.SourceTable),
			ColumnDetailS: c.SourceColumnInfo,
			SchemaNameT:   common.StringUPPER(c.Cfg.MySQLConfig.SchemaName),
			TableNameT:    common.StringUPPER(c.TargetTable),
			ColumnDetailT: c.TargetColumnInfo,
			WhereColumn:   c.WhereColumn,
			WhereRange:    c.WhereRange,
			TaskMode:      c.Cfg.TaskMode,
			TaskStatus:    common.TaskStatusWaiting,
			IsPartition:   c.IsPartition,
		}, &meta.WaitSyncMeta{
			DBTypeS:          c.Cfg.DBTypeS,
			DBTypeT:          c.Cfg.DBTypeT,
			SchemaNameS:      common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:       common.StringUPPER(c.SourceTable),
			TaskMode:         c.Cfg.TaskMode,
			GlobalScnS:       c.SourceGlobalSCN,
			ChunkTotalNums:   1,
			ChunkSuccessNums: 0,
			ChunkFailedNums:  0,
			IsPartition:      c.IsPartition,
		})
		if err != nil {
			return err
		}
		return nil
	}

	// third
	tableRowsByStatistics, err := c.Oracle.GetOracleTableRowsByStatistics(common.StringUPPER(c.Cfg.OracleConfig.SchemaName), c.SourceTable)
	if err != nil {
		return err
	}
	// 统计信息数据行数 0，直接全表扫
	if tableRowsByStatistics == 0 {
		zap.L().Warn("get oracle table rows",
			zap.String("schema", common.StringUPPER(c.Cfg.OracleConfig.SchemaName)),
			zap.String("table", c.SourceTable),
			zap.String("where", "1 = 1"),
			zap.Int("statistics rows", tableRowsByStatistics))
		c.WhereRange = "1 = 1"
		c.WhereColumn = ""
		err = meta.NewCommonModel(c.MetaDB).CreateDataCompareMetaAndUpdateWaitSyncMeta(c.Ctx, &meta.DataCompareMeta{
			DBTypeS:       c.Cfg.DBTypeS,
			DBTypeT:       c.Cfg.DBTypeT,
			SchemaNameS:   common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:    common.StringUPPER(c.SourceTable),
			ColumnDetailS: c.SourceColumnInfo,
			SchemaNameT:   common.StringUPPER(c.Cfg.MySQLConfig.SchemaName),
			TableNameT:    common.StringUPPER(c.TargetTable),
			ColumnDetailT: c.TargetColumnInfo,
			WhereColumn:   c.WhereColumn,
			WhereRange:    c.WhereRange,
			TaskMode:      c.Cfg.TaskMode,
			TaskStatus:    common.TaskStatusWaiting,
			IsPartition:   c.IsPartition,
		}, &meta.WaitSyncMeta{
			DBTypeS:          c.Cfg.DBTypeS,
			DBTypeT:          c.Cfg.DBTypeT,
			SchemaNameS:      common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:       common.StringUPPER(c.SourceTable),
			TaskMode:         c.Cfg.TaskMode,
			GlobalScnS:       c.SourceGlobalSCN,
			ChunkTotalNums:   1,
			ChunkSuccessNums: 0,
			ChunkFailedNums:  0,
			IsPartition:      c.IsPartition,
		})
		if err != nil {
			return err
		}
		return nil
	}

	zap.L().Info("get oracle table statistics rows",
		zap.String("schema", common.StringUPPER(c.Cfg.OracleConfig.SchemaName)),
		zap.String("table", c.SourceTable),
		zap.Int("rows", tableRowsByStatistics))

	// forth
	// indexField > 程序已过滤筛选的字段 DB Filter integer column
	if !strings.EqualFold(customColumn, "") {
		c.WhereColumn = customColumn
	}

	taskName := common.StringsBuilder(common.StringUPPER(c.Cfg.OracleConfig.SchemaName), `_`, c.SourceTable, `_`, `TASK`, strconv.Itoa(c.ChunkID))

	if err = c.Oracle.StartOracleChunkCreateTask(taskName); err != nil {
		return err
	}

	err = c.Oracle.StartOracleCreateChunkByNUMBER(taskName, common.StringUPPER(c.Cfg.OracleConfig.SchemaName), common.StringUPPER(c.SourceTable), c.WhereColumn, strconv.Itoa(c.Cfg.DiffConfig.ChunkSize))
	if err != nil {
		return err
	}

	chunkRes, err := c.Oracle.GetOracleTableChunksByNUMBER(taskName, c.WhereColumn)
	if err != nil {
		return err
	}

	// 判断数据是否存在，更新 data_diff_meta 记录
	if len(chunkRes) == 0 {
		zap.L().Warn("get oracle table rowids rows",
			zap.String("schema", common.StringUPPER(c.Cfg.OracleConfig.SchemaName)),
			zap.String("table", c.SourceTable),
			zap.String("where", "1 = 1"),
			zap.Int("rows", len(chunkRes)))

		c.WhereRange = "1 = 1"
		c.WhereColumn = ""
		err = meta.NewCommonModel(c.MetaDB).CreateDataCompareMetaAndUpdateWaitSyncMeta(c.Ctx, &meta.DataCompareMeta{
			DBTypeS:       c.Cfg.DBTypeS,
			DBTypeT:       c.Cfg.DBTypeT,
			SchemaNameS:   common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:    common.StringUPPER(c.SourceTable),
			ColumnDetailS: c.SourceColumnInfo,
			SchemaNameT:   common.StringUPPER(c.Cfg.MySQLConfig.SchemaName),
			TableNameT:    common.StringUPPER(c.TargetTable),
			ColumnDetailT: c.TargetColumnInfo,
			WhereColumn:   c.WhereColumn,
			WhereRange:    c.WhereRange,
			TaskMode:      c.Cfg.TaskMode,
			TaskStatus:    common.TaskStatusWaiting,
			IsPartition:   c.IsPartition,
		}, &meta.WaitSyncMeta{
			DBTypeS:          c.Cfg.DBTypeS,
			DBTypeT:          c.Cfg.DBTypeT,
			SchemaNameS:      common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:       common.StringUPPER(c.SourceTable),
			TaskMode:         c.Cfg.TaskMode,
			GlobalScnS:       c.SourceGlobalSCN,
			ChunkTotalNums:   1,
			ChunkSuccessNums: 0,
			ChunkFailedNums:  0,
			IsPartition:      c.IsPartition,
		})
		if err != nil {
			return err
		}
		return nil
	}

	var fullMetas []meta.DataCompareMeta
	for _, r := range chunkRes {
		fullMetas = append(fullMetas, meta.DataCompareMeta{
			DBTypeS:       c.Cfg.DBTypeS,
			DBTypeT:       c.Cfg.DBTypeT,
			SchemaNameS:   common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:    common.StringUPPER(c.SourceTable),
			SchemaNameT:   common.StringUPPER(c.Cfg.MySQLConfig.SchemaName),
			TableNameT:    common.StringUPPER(c.TargetTable),
			ColumnDetailS: c.SourceColumnInfo,
			ColumnDetailT: c.TargetColumnInfo,
			WhereRange:    r["CMD"],
			WhereColumn:   c.WhereColumn,
			IsPartition:   c.IsPartition,
			TaskMode:      c.Cfg.TaskMode,
			TaskStatus:    common.TaskStatusWaiting})
	}

	// 防止上游数据少，下游数据多超上游数据边界
	// 获取最小以及最大 Number Column 字段
	querySQL := common.StringsBuilder(`SELECT * FROM `,
		`(SELECT MIN(start_id) START_ID, MAX(end_id) END_ID FROM user_parallel_execute_chunks WHERE task_name = '`, taskName, `')`, ` WHERE ROWNUM = 1`)
	_, res, err := oracle.Query(c.Ctx, c.Oracle.OracleDB, querySQL)
	if err != nil {
		return err
	}

	for _, r := range res {
		fullMetas = append(fullMetas, meta.DataCompareMeta{
			DBTypeS:       c.Cfg.DBTypeS,
			DBTypeT:       c.Cfg.DBTypeT,
			SchemaNameS:   common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:    common.StringUPPER(c.SourceTable),
			SchemaNameT:   common.StringUPPER(c.Cfg.MySQLConfig.SchemaName),
			TableNameT:    common.StringUPPER(c.TargetTable),
			ColumnDetailS: c.SourceColumnInfo,
			ColumnDetailT: c.TargetColumnInfo,
			WhereRange:    common.StringsBuilder(c.WhereColumn, " < ", r["START_ID"]),
			WhereColumn:   c.WhereColumn,
			IsPartition:   c.IsPartition,
			TaskMode:      c.Cfg.TaskMode,
			TaskStatus:    common.TaskStatusWaiting})
		fullMetas = append(fullMetas, meta.DataCompareMeta{
			DBTypeS:       c.Cfg.DBTypeS,
			DBTypeT:       c.Cfg.DBTypeT,
			SchemaNameS:   common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:    common.StringUPPER(c.SourceTable),
			SchemaNameT:   common.StringUPPER(c.Cfg.MySQLConfig.SchemaName),
			TableNameT:    common.StringUPPER(c.TargetTable),
			ColumnDetailS: c.SourceColumnInfo,
			ColumnDetailT: c.TargetColumnInfo,
			WhereRange:    common.StringsBuilder(c.WhereColumn, " > ", res[0]["END_ID"]),
			WhereColumn:   c.WhereColumn,
			IsPartition:   c.IsPartition,
			TaskMode:      c.Cfg.TaskMode,
			TaskStatus:    common.TaskStatusWaiting})
	}

	// 元数据库信息 batch 写入
	err = meta.NewCommonModel(c.MetaDB).BatchCreateDataCompareMetaAndUpdateWaitSyncMeta(c.Ctx,
		fullMetas, c.Cfg.AppConfig.InsertBatchSize, &meta.WaitSyncMeta{
			DBTypeS:          c.Cfg.DBTypeS,
			DBTypeT:          c.Cfg.DBTypeT,
			SchemaNameS:      common.StringUPPER(c.Cfg.OracleConfig.SchemaName),
			TableNameS:       common.StringUPPER(c.SourceTable),
			TaskMode:         c.Cfg.TaskMode,
			GlobalScnS:       c.SourceGlobalSCN,
			ChunkTotalNums:   int64(len(fullMetas)),
			ChunkSuccessNums: 0,
			ChunkFailedNums:  0,
			IsPartition:      c.IsPartition,
		})
	if err != nil {
		return fmt.Errorf("create table [%s.%s] data_diff_meta [batch size] failed: %v", common.StringUPPER(c.Cfg.OracleConfig.SchemaName), c.SourceTable, err)
	}

	if err = c.Oracle.CloseOracleChunkTask(taskName); err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("pre split oracle and mysql table chunk finished",
		zap.String("schema", c.Cfg.OracleConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil

}

func (c *Chunk) String() string {
	jsonByte, _ := json.Marshal(c)
	return string(jsonByte)
}
