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
	"strconv"

	"github.com/WentaoJin/transferdb/util"
)

// 判断下游元数据库全量以及增量数据表是否存在记录
// 1、存在表示之前启动过任务，则根据下游库记录继续运行（safe-mode 默认十分钟)
// 2、不存在表示之前未启动过任务，新开始任务
func (e *Engine) IsExistMySQLTableMetaRecord() (bool, error) {
	tfm := &TableFullMeta{}
	tim := &TableIncrementMeta{}
	tfmRecordCounts, err := tfm.GetTableFullMetaRecordCounts(e)
	if err != nil {
		return false, err
	}

	timRecordCounts, err := tim.GetTableIncrementMetaRecordCounts(e)
	if err != nil {
		return false, err
	}

	switch {
	case tfmRecordCounts == 0 && timRecordCounts == 0:
		return false, nil
	default:
		return true, nil
	}
}

func (e *Engine) GetMySQLTableFullMetaRowIDSQLRecord(schemaName, tableName, queryCond string) ([]string, error) {
	querySQL := fmt.Sprintf(`SELECT
		rowid_sql AS ROWIDSQL
	FROM
		%s %s`, fmt.Sprintf("%s.%s", schemaName, tableName), queryCond)
	_, res, err := Query(e.MysqlDB, querySQL)
	if err != nil {
		return []string{}, err
	}
	var sli []string
	for _, r := range res {
		sli = append(sli, r["ROWIDSQL"])
	}
	return sli, nil
}

// 清理全量同步任务元数据表
// 全量每成功同步一张表，再清理断点记录
func (e *Engine) ClearMySQLTableFullMetaRecord(tableName, deleteSQL string) error {
	_, err := e.MysqlDB.Exec(deleteSQL)
	if err != nil {
		return fmt.Errorf("clear mysql meta schema table [table_full_meta] reocrd with [%s] failed: %v", tableName, err.Error())
	}
	return nil
}

//func (e *Engine) GetMySQLTableFullMetaIDRecordByPage(schemaName, tableName, colName, queryCond string, pageNums int) ([]string, error) {
//	querySQL := fmt.Sprintf(`SELECT
//	%[1]s AS DATA
//FROM
//	( SELECT %[1]s, ROW_NUMBER ( ) OVER ( ORDER BY %[1]s ) AS RowNumber FROM ( SELECT %[1]s FROM %[2]s %[3]s) AS S ) AS T
//WHERE
//	RowNumber %% %[4]d = 1`, colName, fmt.Sprintf("%s.%s", schemaName, tableName), queryCond, pageNums)
//	_, res, err := Query(e.MysqlDB, querySQL)
//	if err != nil {
//		return []string{}, err
//	}
//	var sli []string
//	for _, r := range res {
//		sli = append(sli, r["DATA"])
//	}
//	return sli, nil
//}

func (e *Engine) GetOracleCurrentSnapshotSCN() (string, error) {
	_, res, err := Query(e.OracleDB, `select dbms_flashback.get_system_change_number AS SCN from dual`)
	if err != nil {
		return res[0]["SCN"], err
	}
	return res[0]["SCN"], nil
}

func (e *Engine) InitMySQLTableFullMeta(schemaName, tableName string, extractorBatch int) error {
	tableRows, err := e.getOracleTableRowsByStatistics(schemaName, tableName)
	if err != nil {
		return err
	}
	ok, err := e.isOraclePartitionTable(schemaName, tableName)
	if err != nil {
		return err
	}

	// 用于判断是否需要切分
	// 当行数少于 parallel*10000 则不切分
	parallel := tableRows / extractorBatch

	if tableRows > parallel*10000 {
		if !ok {
			if err := e.getOracleNormalTableTableFullMetaByRowID(schemaName, tableName, parallel); err != nil {
				return err
			}
		} else {
			subPartNames, err := e.getOracleSubPartitionTable(schemaName, tableName)
			if err != nil {
				return err
			}
			for _, subPart := range subPartNames {
				if err := e.getOraclePartitionTableTableFullMetaByRowID(schemaName, tableName, subPart, parallel); err != nil {
					return err
				}
			}
		}
	} else {
		if !ok {
			if err := e.getOracleNormalTableTableFullMetaByRowID(schemaName, tableName, 1); err != nil {
				return err
			}
		} else {
			subPartNames, err := e.getOracleSubPartitionTable(schemaName, tableName)
			if err != nil {
				return err
			}
			for _, subPart := range subPartNames {
				if err := e.getOraclePartitionTableTableFullMetaByRowID(schemaName, tableName, subPart, 1); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (e *Engine) InitMySQLTableIncrementMeta(schemaName, tableName string, globalSCN string) error {
	if err := e.GormDB.Create(&TableIncrementMeta{
		GlobalSCN:        globalSCN,
		SourceSchemaName: schemaName,
		SourceTableName:  tableName,
		CreatedAt:        nil,
		UpdatedAt:        nil,
	}).Error; err != nil {
		return err
	}
	return nil
}

func (e *Engine) GetOracleTableRecordByRowIDSQL(sql string) ([]string, [][]string, error) {
	cols, res, err := QueryRows(e.OracleDB, sql)
	if err != nil {
		return []string{}, [][]string{}, err
	}
	return cols, res, nil
}

func (e *Engine) getOracleNormalTableTableFullMetaByRowID(schemaName, tableName string, parallel int) error {
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
         order by DOI, grp)`, schemaName, tableName, parallel, tableName, schemaName)
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return err
	}

	var (
		fullMetas   []TableFullMeta
		fullMetaIdx []int
	)
	for idx, r := range res {
		fullMetas = append(fullMetas, TableFullMeta{
			SourceSchemaName: schemaName,
			SourceTableName:  tableName,
			RowIdSQL:         r["DATA"],
		})
		fullMetaIdx = append(fullMetaIdx, idx)
	}

	// 划分 batch 数(500)
	if len(fullMetas) <= InsertBatchSize {
		if err := e.GormDB.Create(&fullMetas).Error; err != nil {
			return fmt.Errorf("gorm create table [%s.%s] full meta failed: %v", schemaName, tableName, err)
		}
	} else {
		var fullMetaBatch []TableFullMeta
		splitNums := len(fullMetas) / InsertBatchSize
		splitMetaIdxSlice := util.SplitIntSlice(fullMetaIdx, int64(splitNums))
		for _, ms := range splitMetaIdxSlice {
			for _, idx := range ms {
				fullMetaBatch = append(fullMetaBatch, fullMetas[idx])
			}
			if err := e.GormDB.Create(&fullMetaBatch).Error; err != nil {
				return fmt.Errorf("gorm create table [%s.%s] full meta failed: %v", schemaName, tableName, err)
			}
		}
	}

	return nil
}

func (e *Engine) getOraclePartitionTableTableFullMetaByRowID(schemaName, tableName, partTableName string, parallel int) error {
	querySQL := fmt.Sprintf(`select rownum,'select * from %s.%s rowid between ' || chr(39) ||
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
                 WHERE ext.segment_name = UPPER('%s')           --物理表
                   AND ext.owner = UPPER('%s')                         --schema
                   AND obj.owner = ext.owner
                   AND obj.object_name = ext.segment_name
                   AND obj.DATA_OBJECT_ID IS NOT NULL
                   AND obj.subobject_name = UPPER('%s') --分区表
                 ORDER BY DATA_OBJECT_ID, relative_fno, block_id)
         order by DOI, grp)`, schemaName, tableName, parallel, tableName, schemaName, partTableName)
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return err
	}
	var (
		fullMetas   []TableFullMeta
		fullMetaIdx []int
	)
	for idx, r := range res {
		fullMetas = append(fullMetas, TableFullMeta{
			SourceSchemaName: schemaName,
			SourceTableName:  tableName,
			RowIdSQL:         r["DATA"],
		})
		fullMetaIdx = append(fullMetaIdx, idx)
	}

	// 划分 batch 数(500)
	if len(fullMetas) <= InsertBatchSize {
		if err := e.GormDB.Create(&fullMetas).Error; err != nil {
			return fmt.Errorf("gorm create partiton table [%s.%s] full meta failed: %v", schemaName, tableName, err)
		}
	} else {
		var fullMetaBatch []TableFullMeta
		splitNums := len(fullMetas) / InsertBatchSize
		splitMetaIdxSlice := util.SplitIntSlice(fullMetaIdx, int64(splitNums))
		for _, ms := range splitMetaIdxSlice {
			for _, idx := range ms {
				fullMetaBatch = append(fullMetaBatch, fullMetas[idx])
			}
			if err := e.GormDB.Create(&fullMetaBatch).Error; err != nil {
				return fmt.Errorf("gorm create partiton table [%s.%s] full meta failed: %v", schemaName, tableName, err)
			}
		}
	}
	return nil
}

func (e *Engine) getOracleTableRowsByStatistics(schemaName, tableName string) (int, error) {
	querySQL := fmt.Sprintf(`select NUM_ROWS
  from all_tables
 where upper(OWNER) = upper('%s')
   and upper(table_name) = upper('%s')`, schemaName, tableName)
	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return 0, err
	}
	numRows, err := strconv.Atoi(res[0]["NUM_ROWS"])
	if err != nil {
		return 0, err
	}
	return numRows, nil
}

func (e *Engine) isOraclePartitionTable(schemaName, tableName string) (bool, error) {
	_, res, err := Query(e.OracleDB, fmt.Sprintf(`select count(1) AS COUNT
  from dba_tables
 where partitioned = 'YES'
   and upper(owner) = upper('%s')
   and upper(table_name) = upper('%s')`, schemaName, tableName))
	if err != nil {
		return false, err
	}
	if res[0]["COUNT"] == "0" {
		return false, nil
	}
	return true, nil
}

func (e *Engine) getOracleSubPartitionTable(schemaName, tableName string) ([]string, error) {
	_, res, err := Query(e.OracleDB, fmt.Sprintf(`select PARTITION_NAME
  from ALL_TAB_PARTITIONS
 where upper(TABLE_OWNER) = upper('%s')
   AND upper(table_name) = upper('%s');
`, schemaName, tableName))
	if err != nil {
		return []string{}, err
	}
	var partsName []string
	for _, r := range res {
		partsName = append(partsName, r["PARTITION_NAME"])
	}
	return partsName, nil
}
