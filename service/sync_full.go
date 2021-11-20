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
package service

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/wentaojin/transferdb/utils"

	"gorm.io/gorm"

	"go.uber.org/zap"
)

// 该函数只应用于全量同步模式或者 ALL 同步模式
// 1、断点续传判断，判断是否可进行断点续传
// 2、判断是否存在未初始化元信息的表
func (e *Engine) JudgingCheckpointResume(schemaName string, tableMetas []WaitSyncMeta) ([]string, error) {
	var panicTblFullSlice []string

	tfm := &FullSyncMeta{}
	for _, table := range tableMetas {
		fullRecordCounts, err := tfm.GetFullSyncMetaRecordCounts(schemaName, table.SourceTableName, e)
		if err != nil {
			return panicTblFullSlice, err
		}

		if fullRecordCounts != table.FullSplitTimes {
			panicTblFullSlice = append(panicTblFullSlice, table.SourceTableName)
		}
	}
	return panicTblFullSlice, nil
}

func (e *Engine) IsExistIncrementSyncMetaRecord(schemaName string, transferTableSlice []string) ([]string, []string, error) {
	var (
		notExistRecords []string
		existRecords    []string
	)
	for _, tbl := range transferTableSlice {
		tim := &IncrementSyncMeta{}
		counts, err := tim.GetIncrementSyncMetaRecordCounts(schemaName, tbl, e)
		if err != nil {
			return existRecords, notExistRecords, err
		}

		// 表不存在或者表数异常
		if counts == 0 || counts > 1 {
			notExistRecords = append(notExistRecords, tbl)
		}
		if counts == 1 {
			existRecords = append(existRecords, tbl)
		}
	}
	return existRecords, notExistRecords, nil
}

// 清理并更新同步任务元数据表
// 1、全量每成功同步一张表记录，再清理记录
// 2、更新同步数据表元信息
func (e *Engine) ModifyWaitAndFullSyncTableMetaRecord(metaSchemaName, sourceSchemaName, sourceTableName, rowidSQL, syncMode string) error {
	if err := e.GormDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(FullSyncMeta{}).
			Where(`source_schema_name = ? AND source_table_name= ? AND upper(rowid_sql)= ?`,
				strings.ToUpper(sourceSchemaName),
				strings.ToUpper(sourceTableName),
				strings.ToUpper(rowidSQL)).Delete(&FullSyncMeta{}).Error; err != nil {
			return fmt.Errorf(
				`clear mysql meta schema [%s] table [full_sync_meta] reocrd with source table [%s] failed: %v`,
				metaSchemaName, sourceTableName, err.Error())
		}
		Logger.Info("clear mysql meta",
			zap.String("schema", sourceSchemaName),
			zap.String("table", sourceTableName),
			zap.String("sql", rowidSQL),
			zap.String("status", "success"))

		if err := tx.Model(&WaitSyncMeta{}).
			Where(`source_schema_name = ? AND source_table_name= ? AND sync_mode = ?`,
				strings.ToUpper(sourceSchemaName),
				strings.ToUpper(sourceTableName),
				syncMode).
			Update("full_split_times", gorm.Expr("full_split_times - 1")).Error; err != nil {
			return err

		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// 清理全量同步任务元数据表
func (e *Engine) TruncateFullSyncTableMetaRecord(metaSchemaName string) error {
	if err := e.GormDB.Exec(fmt.Sprintf("TRUNCATE TABLE %s.full_sync_meta", metaSchemaName)).Error; err != nil {
		return fmt.Errorf("truncate mysql meta schema table [full_sync_meta] reocrd failed: %v", err.Error())
	}

	Logger.Info("truncate table full meta record",
		zap.String("schema", metaSchemaName),
		zap.String("table", "full_sync_meta"),
		zap.String("status", "success"))
	return nil
}

func (e *Engine) DeleteWaitSyncTableMetaRecord(metaSchemaName, sourceSchemaName, sourceTableName, syncMode string) error {
	if err := e.GormDB.Where("source_schema_name = ? AND source_table_name = ? AND sync_mode = ?",
		strings.ToUpper(sourceSchemaName),
		strings.ToUpper(sourceTableName),
		syncMode).Delete(&WaitSyncMeta{}).Error; err != nil {
		return err
	}
	Logger.Info("delete table record",
		zap.String("schema", metaSchemaName),
		zap.String("table", "wait_sync_meta"),
		zap.String("record", fmt.Sprintf("%s.%s", sourceSchemaName, sourceTableName)),
		zap.String("status", "success"))
	return nil
}

func (e *Engine) TruncateMySQLTableRecord(targetSchemaName string, tableName string) error {
	if err := e.GormDB.Exec(fmt.Sprintf("TRUNCATE TABLE %s.%s", targetSchemaName, tableName)).Error; err != nil {
		return fmt.Errorf("truncate mysql meta schema table [%v] reocrd failed: %v", tableName, err.Error())
	}
	Logger.Info("truncate table",
		zap.String("schema", targetSchemaName),
		zap.String("table", tableName),
		zap.String("status", "success"))

	return nil
}

func (e *Engine) InitWaitAndFullSyncMetaRecord(schemaName, tableName string, globalSCN int, extractorBatch, insertBatchSize int, syncMode string) error {
	tableRows, err := e.getOracleTableRowsByStatistics(schemaName, tableName)
	if err != nil {
		return err
	}

	// 用于判断是否需要切分
	// 当行数少于 parallel*10000 则不切分
	Logger.Info("get oracle table statistics rows",
		zap.String("schema", schemaName),
		zap.String("table", tableName),
		zap.Int("rows", tableRows))

	parallel := math.Ceil(float64(tableRows) / float64(extractorBatch))

	// Oracle 表数据切分数根据 sum(数据块数) / parallel
	if tableRows > extractorBatch {
		rowCounts, err := e.getAndInitOracleNormalTableFullSyncMetaUsingRowID(schemaName, tableName, globalSCN, int(parallel), insertBatchSize)
		if err != nil {
			return err
		}
		if err := e.UpdateWaitSyncMetaTableRecord(schemaName, tableName, rowCounts, globalSCN, syncMode); err != nil {
			return err
		}
	} else {
		rowCounts, err := e.getAndInitOracleNormalTableFullSyncMetaUsingRowID(schemaName, tableName, globalSCN, 1, insertBatchSize)
		if err != nil {
			return err
		}
		if err := e.UpdateWaitSyncMetaTableRecord(schemaName, tableName, rowCounts, globalSCN, syncMode); err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) InitWaitSyncTableMetaRecord(schemaName string, tableName []string, syncMode string) error {
	// 初始同步表全量任务为 -1 表示未进行全量初始化，初始化完成会变更
	// 全量同步完成，增量阶段，值预期都是 0
	for _, table := range tableName {
		if err := e.GormDB.Create(&WaitSyncMeta{
			SourceSchemaName: strings.ToUpper(schemaName),
			SourceTableName:  strings.ToUpper(table),
			SyncMode:         syncMode,
			FullGlobalSCN:    -1,
			FullSplitTimes:   -1,
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) UpdateWaitSyncMetaTableRecord(schemaName, tableName string, rowCounts, globalSCN int, syncMode string) error {
	if err := e.GormDB.Model(&WaitSyncMeta{}).
		Where("source_schema_name = ? AND source_table_name = ? AND sync_mode = ?",
			strings.ToUpper(schemaName),
			strings.ToUpper(tableName),
			syncMode).
		Updates(WaitSyncMeta{
			FullGlobalSCN:  globalSCN,
			FullSplitTimes: rowCounts,
		}).Error; err != nil {
		return err
	}
	return nil
}

func (e *Engine) InitIncrementSyncMetaRecord(schemaName, tableName string, globalSCN int) error {
	if err := e.GormDB.Create(&IncrementSyncMeta{
		GlobalSCN:        globalSCN,
		SourceSchemaName: strings.ToUpper(schemaName),
		SourceTableName:  strings.ToUpper(tableName),
		SourceTableSCN:   globalSCN,
	}).Error; err != nil {
		return err
	}
	return nil
}

func (e *Engine) GetWaitSyncTableMetaRecord(schemaName string, syncMode string) ([]WaitSyncMeta, []string, error) {
	var (
		tableMetas []WaitSyncMeta
		tables     []string
	)
	if err := e.GormDB.Where("source_schema_name = ? AND full_global_scn = -1 AND full_split_times = -1 and sync_mode = ?",
		strings.ToUpper(schemaName), syncMode).Find(&tableMetas).Error; err != nil {
		return tableMetas, tables, err
	}
	if len(tableMetas) > 0 {
		for _, table := range tableMetas {
			tables = append(tables, strings.ToUpper(table.SourceTableName))
		}
	}
	return tableMetas, tables, nil
}

func (e *Engine) GetFinishFullSyncMetaRecord(schemaName string, syncMode string) ([]WaitSyncMeta, []string, error) {
	var (
		tableMetas []WaitSyncMeta
		tables     []string
	)
	if err := e.GormDB.Where("source_schema_name = ? AND full_global_scn > -1 AND full_split_times = 0 and sync_mode = ?",
		strings.ToUpper(schemaName),
		syncMode).Find(&tableMetas).Error; err != nil {
		return tableMetas, tables, err
	}
	if len(tableMetas) > 0 {
		for _, table := range tableMetas {
			tables = append(tables, strings.ToUpper(table.SourceTableName))
		}
	}
	return tableMetas, tables, nil
}

func (e *Engine) IsFinishFullSyncMetaRecord(schemaName string, transferTableSlice []string, syncMode string) ([]string, error) {
	var (
		panicTables []string
		tableMetas  []WaitSyncMeta
	)
	for _, tbl := range transferTableSlice {
		if err := e.GormDB.Where("source_schema_name = ? AND source_table_name = ? AND full_global_scn > -1 AND full_split_times = 0 and sync_mode = ?",
			strings.ToUpper(schemaName),
			strings.ToUpper(tbl),
			syncMode).Find(&tableMetas).Error; err != nil {
			return panicTables, err
		}
		if len(tableMetas) == 0 {
			panicTables = append(panicTables, tbl)
		}
	}
	return panicTables, nil
}

func (e *Engine) GetPartSyncTableMetaRecord(schemaName, syncMode string) ([]WaitSyncMeta, []string, error) {
	var (
		tableMetas []WaitSyncMeta
		tables     []string
	)
	if err := e.GormDB.Where("source_schema_name = ? AND full_global_scn > -1 AND full_split_times > 0 and sync_mode = ?",
		strings.ToUpper(schemaName),
		syncMode).Find(&tableMetas).Error; err != nil {
		return tableMetas, tables, err
	}
	if len(tableMetas) > 0 {
		for _, table := range tableMetas {
			tables = append(tables, strings.ToUpper(table.SourceTableName))
		}
	}
	return tableMetas, tables, nil
}

func (e *Engine) IsExistWaitSyncTableMetaRecord(schemaName string, tableName, syncMode string) (bool, error) {
	var tableMetas []WaitSyncMeta

	if err := e.GormDB.Where("source_schema_name = ? AND source_table_name = ? AND sync_mode = ?",
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName),
		syncMode).Find(&tableMetas).Error; err != nil {
		return false, err
	}
	if len(tableMetas) > 0 {
		return true, nil
	}
	return false, nil
}

func (e *Engine) GetFullSyncMetaRowIDRecord(schemaName, tableName string) ([]string, error) {
	var (
		rowID          []string
		tableFullMetas []FullSyncMeta
	)
	if err := e.GormDB.
		Where("source_schema_name = ? AND source_table_name = ?",
			strings.ToUpper(schemaName), strings.ToUpper(tableName)).Find(&tableFullMetas).Error; err != nil {
		return rowID, err
	}
	for _, rowSQL := range tableFullMetas {
		rowID = append(rowID, rowSQL.RowidSQL)
	}
	return rowID, nil
}

func (e *Engine) GetOracleCurrentSnapshotSCN() (int, error) {
	// 获取当前 SCN 号
	_, res, err := Query(e.OracleDB, "select min(current_scn) CURRENT_SCN from gv$database")
	var globalSCN int
	if err != nil {
		return globalSCN, err
	}
	globalSCN, err = strconv.Atoi(res[0]["CURRENT_SCN"])
	if err != nil {
		return globalSCN, err
	}
	return globalSCN, nil
}

func (e *Engine) GetOracleTableRecordByRowIDSQL(sql string) ([]string, []string, error) {
	Logger.Info("exec sql",
		zap.String("sql", sql))
	cols, res, err := e.QueryFormatOracleRows(sql)
	if err != nil {
		return []string{}, []string{}, err
	}
	return cols, res, nil
}

func (e *Engine) getAndInitOracleNormalTableFullSyncMetaUsingRowID(
	schemaName,
	tableName string,
	globalSCN int, parallel, insertBatchSize int) (int, error) {
	querySQL := fmt.Sprintf(`select rownum,
       'select * from %s.%s where rowid between ' || chr(39) ||
       dbms_rowid.rowid_create(1, DOI, lo_fno, lo_block, 0) || chr(39) ||
       ' and  ' || chr(39) ||
       dbms_rowid.rowid_create(1, DOI, hi_fno, hi_block, 1000000) ||
       chr(39) DATA
  from (SELECT DISTINCT DOI,
                        grp,
                        first_value(relative_fno) over(partition BY DOI, grp order by relative_fno, block_id rows BETWEEN unbounded preceding AND unbounded following) lo_fno,
                        first_value(block_id) over(partition BY DOI, grp order by relative_fno, block_id rows BETWEEN unbounded preceding AND unbounded following) lo_block,
                        last_value(relative_fno) over(partition BY DOI, grp order by relative_fno, block_id rows BETWEEN unbounded preceding AND unbounded following) hi_fno,
                        last_value(block_id + blocks - 1) over(partition BY DOI, grp order by relative_fno, block_id rows BETWEEN unbounded preceding AND unbounded following) hi_block,
                        SUM(blocks) over(partition BY DOI, grp) sum_blocks,
                        SUBOBJECT_NAME
          FROM (SELECT obj.OBJECT_ID,
                       obj.SUBOBJECT_NAME,
                       obj.DATA_OBJECT_ID as DOI,
                       ext.relative_fno,
                       ext.block_id,
                       (SUM(blocks) over()) SUM,
                       (SUM(blocks)
                        over(ORDER BY DATA_OBJECT_ID, relative_fno, block_id) - 0.01) sum_fno,
                       TRUNC((SUM(blocks) over(ORDER BY DATA_OBJECT_ID,
                                               relative_fno,
                                               block_id) - 0.01) /
                             (SUM(blocks) over() / %d)) grp,
                       ext.blocks
                   FROM dba_extents ext, dba_objects obj
                 WHERE UPPER(ext.segment_name) = UPPER('%s')
                   AND UPPER(ext.owner) = UPPER('%s')
                   AND obj.owner = ext.owner
                   AND obj.object_name = ext.segment_name
                   AND obj.DATA_OBJECT_ID IS NOT NULL
                 ORDER BY DATA_OBJECT_ID, relative_fno, block_id)
         order by DOI, grp)`, strings.ToUpper(schemaName), strings.ToUpper(tableName), parallel, strings.ToUpper(tableName), strings.ToUpper(schemaName))

	var rowCount int

	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return rowCount, err
	}

	// 判断数据是否存在，不存在直接跳过
	if len(res) == 0 {
		return rowCount, nil
	}

	var (
		fullMetas   []FullSyncMeta
		fullMetaIdx []int
	)
	for idx, r := range res {
		fullMetas = append(fullMetas, FullSyncMeta{
			SourceSchemaName: strings.ToUpper(schemaName),
			SourceTableName:  strings.ToUpper(tableName),
			RowidSQL:         r["DATA"],
			IsPartition:      "N",
			GlobalSCN:        globalSCN,
		})
		fullMetaIdx = append(fullMetaIdx, idx)
	}

	// 划分 batch 数(500)
	if len(fullMetas) <= insertBatchSize {
		if err := e.GormDB.Create(&fullMetas).Error; err != nil {
			return len(res), fmt.Errorf("gorm create table [%s.%s] table_full_meta failed: %v", schemaName, tableName, err)
		}
	} else {
		var fullMetaBatch []FullSyncMeta
		splitNums := len(fullMetas) / insertBatchSize
		splitMetaIdxSlice := utils.SplitIntSlice(fullMetaIdx, int64(splitNums))
		for _, ms := range splitMetaIdxSlice {
			for _, idx := range ms {
				fullMetaBatch = append(fullMetaBatch, fullMetas[idx])
			}
			if err := e.GormDB.Create(&fullMetaBatch).Error; err != nil {
				return len(res), fmt.Errorf("gorm create table [%s.%s] table_full_meta failed: %v", schemaName, tableName, err)
			}
		}
	}

	return len(res), nil
}

func (e *Engine) getOracleTableRowsByStatistics(schemaName, tableName string) (int, error) {
	querySQL := fmt.Sprintf(`select NVL(NUM_ROWS,0) AS NUM_ROWS
  from all_tables
 where upper(OWNER) = upper('%s')
   and upper(table_name) = upper('%s')`, schemaName, tableName)
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return 0, err
	}

	numRows, err := strconv.Atoi(res[0]["NUM_ROWS"])
	if err != nil {
		return 0, fmt.Errorf("get oracle schema table [%v] rows by statistics falied: %v",
			fmt.Sprintf("%s.%s", schemaName, tableName), err)
	}
	return numRows, nil
}
