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
package db

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"gorm.io/gorm"

	"github.com/wentaojin/transferdb/zlog"
	"go.uber.org/zap"

	"github.com/wentaojin/transferdb/util"
)

// 该函数只应用于全量同步模式或者 ALL 同步模式
// 1、断点续传判断，判断是否可进行断点续传
// 2、判断是否存在未初始化元信息的表
func (e *Engine) AdjustFullStageMySQLTableMetaRecord(schemaName string, tableMetas []TableMeta) (bool, []string, []string, error) {
	var (
		panicTblFullSlice []string
		tblFullSlice      []string
	)

	tfm := &TableFullMeta{}
	for _, table := range tableMetas {
		tfmRecordCounts, err := tfm.GetTableFullMetaRecordCounts(schemaName, table.SourceTableName, e)
		if err != nil {
			return false, tblFullSlice, panicTblFullSlice, err
		}

		// 首次运行待初始化表
		if tfmRecordCounts == 0 && table.FullSplitTimes == -1 {
			tblFullSlice = append(tblFullSlice, table.SourceTableName)
		} else {
			// 异常表
			if tfmRecordCounts != table.FullSplitTimes {
				panicTblFullSlice = append(panicTblFullSlice, table.SourceTableName)
			}
		}
	}
	if len(panicTblFullSlice) != 0 {
		return false, tblFullSlice, panicTblFullSlice, nil
	}
	return true, tblFullSlice, panicTblFullSlice, nil
}

func (e *Engine) IsNotExistMySQLTableIncrementMetaRecord() (bool, error) {
	tim := &TableIncrementMeta{}
	count, err := tim.GetTableIncrementMetaRowCounts(e)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return true, nil
	}
	return false, nil
}

// 清理并更新同步任务元数据表
// 1、全量每成功同步一张表记录，再清理记录
// 2、更新同步数据表元信息
func (e *Engine) ModifyMySQLTableMetaRecord(metaSchemaName, sourceSchemaName, sourceTableName, rowidSQL string) error {
	if err := e.GormDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(TableFullMeta{}).
			Where(`source_schema_name = ? AND source_table_name= ? AND upper(rowid_sql)= ?`,
				strings.ToUpper(sourceSchemaName),
				strings.ToUpper(sourceTableName),
				strings.ToUpper(rowidSQL)).Delete(&TableFullMeta{}).Error; err != nil {
			return fmt.Errorf(
				`clear mysql meta schema [%s] table [table_full_meta] reocrd with source table [%s] failed: %v`,
				metaSchemaName, sourceTableName, err.Error())
		}
		zlog.Logger.Info("clear mysql meta",
			zap.String("schema", sourceSchemaName),
			zap.String("table", sourceTableName),
			zap.String("sql", rowidSQL),
			zap.String("status", "success"))

		if err := tx.Model(&TableMeta{}).
			Where(`source_schema_name = ? AND source_table_name= ?`,
				strings.ToUpper(sourceSchemaName),
				strings.ToUpper(sourceTableName)).
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
func (e *Engine) TruncateMySQLTableFullMetaRecord(metaSchemaName string) error {
	if err := e.GormDB.Exec(fmt.Sprintf("TRUNCATE TABLE %s.table_full_meta", metaSchemaName)).Error; err != nil {
		return fmt.Errorf("truncate mysql meta schema table [table_full_meta] reocrd failed: %v", err.Error())
	}

	zlog.Logger.Info("truncate table full meta record",
		zap.String("schema", metaSchemaName),
		zap.String("table", "table_full_meta"),
		zap.String("status", "success"))
	return nil
}

func (e *Engine) ClearMySQLTableMetaRecord(metaSchemaName, sourceSchemaName string) error {
	if err := e.GormDB.Model(&TableMeta{}).
		Where("source_schema_name = ?",
			strings.ToUpper(sourceSchemaName)).
		Updates(TableMeta{
			FullGlobalSCN:  -1,
			FullSplitTimes: -1,
		}).Error; err != nil {
		return err
	}
	zlog.Logger.Info("modify table meta record",
		zap.String("schema", metaSchemaName),
		zap.String("table", "table_meta"),
		zap.String("status", "success"))
	return nil
}

func (e *Engine) TruncateMySQLTableRecord(targetSchemaName string, tableMetas []TableMeta) error {
	for _, table := range tableMetas {
		if err := e.GormDB.Exec(fmt.Sprintf("TRUNCATE TABLE %s.%s", targetSchemaName, table.SourceTableName)).Error; err != nil {
			return fmt.Errorf("truncate mysql meta schema table [%v] reocrd failed: %v", table.SourceTableName, err.Error())
		}
		zlog.Logger.Info("truncate table record",
			zap.String("schema", targetSchemaName),
			zap.String("table", table.SourceTableName),
			zap.String("status", "success"))
	}

	return nil
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

func (e *Engine) InitMySQLTableFullMeta(schemaName, tableName string, globalSCN int, extractorBatch, insertBatchSize int) error {
	tableRows, err := e.getOracleTableRowsByStatistics(schemaName, tableName)
	if err != nil {
		return err
	}

	// 用于判断是否需要切分
	// 当行数少于 parallel*10000 则不切分
	zlog.Logger.Info("get oracle table statistics rows",
		zap.String("schema", schemaName),
		zap.String("table", tableName),
		zap.Int("rows", tableRows))

	parallel := math.Ceil(float64(tableRows) / float64(extractorBatch))

	// Oracle 表数据切分数根据 sum(数据块数) / parallel
	if tableRows > extractorBatch {
		rowCounts, err := e.getAndInitOracleNormalTableTableFullMetaByRowID(schemaName, tableName, globalSCN, int(parallel), insertBatchSize)
		if err != nil {
			return err
		}
		if err := e.UpdateMySQLTableMetaRecord(schemaName, tableName, rowCounts, globalSCN); err != nil {
			return err
		}
	} else {
		rowCounts, err := e.getAndInitOracleNormalTableTableFullMetaByRowID(schemaName, tableName, globalSCN, 1, insertBatchSize)
		if err != nil {
			return err
		}
		if err := e.UpdateMySQLTableMetaRecord(schemaName, tableName, rowCounts, globalSCN); err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) InitMySQLTableMetaRecord(schemaName string, tableName []string) error {
	// 初始同步表全量任务为 -1 表示未进行全量初始化，初始化完成会变更
	// 全量同步完成，增量阶段，值预期都是 0
	for _, table := range tableName {
		if err := e.GormDB.Create(&TableMeta{
			SourceSchemaName: strings.ToUpper(schemaName),
			SourceTableName:  strings.ToUpper(table),
			FullGlobalSCN:    -1,
			FullSplitTimes:   -1,
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) UpdateMySQLTableMetaRecord(schemaName, tableName string, rowCounts, globalSCN int) error {
	if err := e.GormDB.Model(&TableMeta{}).
		Where("source_schema_name = ? AND source_table_name = ?",
			strings.ToUpper(schemaName),
			strings.ToUpper(tableName)).
		Updates(TableMeta{
			FullGlobalSCN:  globalSCN,
			FullSplitTimes: rowCounts,
		}).Error; err != nil {
		return err
	}
	return nil
}

func (e *Engine) InitMySQLTableIncrementMeta(schemaName, tableName string, globalSCN int) error {
	if err := e.GormDB.Create(&TableIncrementMeta{
		GlobalSCN:        globalSCN,
		SourceSchemaName: strings.ToUpper(schemaName),
		SourceTableName:  strings.ToUpper(tableName),
		SourceTableSCN:   globalSCN,
	}).Error; err != nil {
		return err
	}
	return nil
}

func (e *Engine) GetMySQLTableMetaRecord(schemaName string) ([]TableMeta, []string, error) {
	var (
		tableMetas []TableMeta
		tables     []string
	)
	if err := e.GormDB.Where("source_schema_name = ?",
		strings.ToUpper(schemaName)).Find(&tableMetas).Error; err != nil {
		return tableMetas, tables, err
	}
	if len(tableMetas) > 0 {
		for _, table := range tableMetas {
			tables = append(tables, strings.ToUpper(table.SourceTableName))
		}
	}
	return tableMetas, tables, nil
}

func (e *Engine) GetMySQLTableFullMetaSchemaTableRecord(schemaName string) ([]string, error) {
	var schemaTables []string
	if err := e.GormDB.Model(&TableFullMeta{}).
		Distinct().
		Pluck("source_table_name", &schemaTables).
		Where("upper(source_schema_name) = ?",
			strings.ToUpper(schemaName)).Error; err != nil {
		return []string{}, err
	}
	return schemaTables, nil
}

func (e *Engine) GetMySQLTableFullMetaRowIDRecord(schemaName, tableName string) ([]string, error) {
	var (
		rowID          []string
		tableFullMetas []TableFullMeta
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

func (e *Engine) GetOracleTableRecordByRowIDSQL(sql string) ([]string, []string, error) {
	zlog.Logger.Info("exec sql",
		zap.String("sql", sql))
	cols, res, err := e.QueryFormatOracleRows(sql)
	if err != nil {
		return []string{}, []string{}, err
	}
	return cols, res, nil
}

func (e *Engine) getAndInitOracleNormalTableTableFullMetaByRowID(
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
		fullMetas   []TableFullMeta
		fullMetaIdx []int
	)
	for idx, r := range res {
		fullMetas = append(fullMetas, TableFullMeta{
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
		var fullMetaBatch []TableFullMeta
		splitNums := len(fullMetas) / insertBatchSize
		splitMetaIdxSlice := util.SplitIntSlice(fullMetaIdx, int64(splitNums))
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
