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
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/wentaojin/transferdb/utils"
	"go.uber.org/zap"
)

func (e *Engine) GetMySQLTableName(schemaName, tableName string) ([]string, error) {
	_, res, err := Query(e.MysqlDB, fmt.Sprintf(`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES where UPPER(TABLE_SCHEMA) = '%s' AND UPPER(TABLE_NAME) IN (%s)`, strings.ToUpper(schemaName), strings.ToUpper(tableName)))
	if err != nil {
		return []string{}, err
	}
	if len(res) == 0 {
		return []string{}, nil
	}

	var tbls []string
	for _, r := range res {
		tbls = append(tbls, r["TABLE_NAME"])
	}

	return tbls, nil
}

func (e *Engine) InitDataDiffMetaRecordByNUMBER(sourceSchema, sourceTable, sourceColumnInfo, targetColumnInfo, numberColName string,
	globalSCN, workerID, chunkSize, insertBatchSize int, syncMode string) error {
	tableRows, isPartition, err := e.getOracleTableRowsByStatistics(sourceSchema, sourceTable)
	if err != nil {
		return err
	}

	// 统计信息数据行数 0，直接全表扫
	if tableRows == 0 {
		Logger.Warn("get oracle table rows",
			zap.String("schema", sourceSchema),
			zap.String("table", sourceTable),
			zap.String("where", "1 = 1"),
			zap.Int("statistics rows", tableRows))

		if err = e.GormDB.Create(&DataDiffMeta{
			SourceSchemaName: strings.ToUpper(sourceSchema),
			SourceTableName:  strings.ToUpper(sourceTable),
			SourceColumnInfo: sourceColumnInfo,
			TargetColumnInfo: targetColumnInfo,
			Range:            "1 = 1",
			NumberColumn:     "",
			IsPartition:      isPartition,
		}).Error; err != nil {
			return fmt.Errorf("gorm create table [%s.%s] data_diff_meta failed [statistics rows = 0]: %v", sourceSchema, sourceTable, err)
		}

		if err = e.UpdateWaitSyncMetaTableRecord(sourceSchema, sourceTable, tableRows, globalSCN, isPartition, syncMode); err != nil {
			return err
		}
		return nil
	}

	Logger.Info("get oracle table statistics rows",
		zap.String("schema", sourceSchema),
		zap.String("table", sourceTable),
		zap.Int("rows", tableRows))

	taskName := utils.StringsBuilder(sourceSchema, `_`, sourceTable, `_`, `TASK`, strconv.Itoa(workerID))

	if err = e.StartOracleChunkCreateTask(taskName); err != nil {
		return err
	}

	if err = e.StartOracleCreateChunkByNUMBER(taskName, strings.ToUpper(sourceSchema), strings.ToUpper(sourceTable), numberColName, strconv.Itoa(chunkSize)); err != nil {
		return err
	}

	rowCounts, err := e.GetOracleTableChunksByNUMBER(taskName, strings.ToUpper(sourceSchema), strings.ToUpper(sourceTable),
		sourceColumnInfo, targetColumnInfo, numberColName, insertBatchSize, isPartition)
	if err != nil {
		return err
	}

	if err = e.UpdateWaitSyncMetaTableRecord(sourceSchema, sourceTable, rowCounts, globalSCN, isPartition, syncMode); err != nil {
		return err
	}

	if err = e.CloseOracleChunkTask(taskName); err != nil {
		return err
	}
	return nil
}

func (e *Engine) InitDataDiffMetaRecordByWhere(sourceSchema, sourceTable, sourceColumnInfo, targetColumnInfo, whereS, syncMode string, globalSCN int) error {
	_, isPartition, err := e.getOracleTableRowsByStatistics(sourceSchema, sourceTable)
	if err != nil {
		return err
	}

	if err = e.GormDB.Create(&DataDiffMeta{
		SourceSchemaName: strings.ToUpper(sourceSchema),
		SourceTableName:  strings.ToUpper(sourceTable),
		SourceColumnInfo: sourceColumnInfo,
		TargetColumnInfo: targetColumnInfo,
		Range:            whereS,
		NumberColumn:     "",
		IsPartition:      isPartition,
	}).Error; err != nil {
		return fmt.Errorf("gorm create table [%s.%s] data_diff_meta failed [whereS]: %v", sourceSchema, sourceTable, err)
	}

	if err = e.UpdateWaitSyncMetaTableRecord(sourceSchema, sourceTable, 1, globalSCN, isPartition, syncMode); err != nil {
		return err
	}
	return nil
}

func (e *Engine) IsNumberColumnTYPE(schemaName, tableName, indexFiledName string) (bool, error) {
	querySQL := fmt.Sprintf(`select t.COLUMN_NAME,t.DATA_TYPE
	from dba_tab_columns t, dba_col_comments c
	where t.table_name = c.table_name
	and t.column_name = c.column_name
	and t.owner = c.owner
	and upper(t.owner) = upper('%s')
	and upper(t.table_name) = upper('%s')
	and upper(t.column_name) = upper('%s')`,
		strings.ToUpper(schemaName),
		strings.ToUpper(tableName),
		strings.ToUpper(indexFiledName))

	_, queryRes, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return false, err
	}
	if len(queryRes) == 0 || len(queryRes) > 1 {
		return false, fmt.Errorf("oracle table [%s.%s] column [%s] isn't exist or query result is multiple, please check again", schemaName, tableName, indexFiledName)
	}
	for _, q := range queryRes {
		if strings.ToUpper(q["COLUMN_NAME"]) == strings.ToUpper(indexFiledName) && strings.ToUpper(q["DATA_TYPE"]) == "NUMBER" {
			return true, nil
		}
	}
	return false, nil
}

func (e *Engine) StartOracleCreateChunkByNUMBER(taskName, schemaName, tableName, numberColName string, chunkSize string) error {
	ctx, _ := context.WithCancel(context.Background())

	chunkSQL := utils.StringsBuilder(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_NUMBER_COL (task_name   => '`, taskName, `',
                                               table_owner => '`, schemaName, `',
                                               table_name  => '`, tableName, `',
                                               table_column => '`, numberColName, `',
                                               chunk_size  => `, chunkSize, `);
END;`)
	_, err := e.OracleDB.ExecContext(ctx, chunkSQL)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create_chunks_by_rowid task failed: %v, sql: %v", err, chunkSQL)
	}
	return nil
}

func (e *Engine) GetOracleTableChunksByNUMBER(taskName, sourceSchema, sourceTable, sourceColumnInfo, targetColumnInfo, numberColName string,
	insertBatchSize int, isPartition string) (int, error) {
	var rowCount int

	querySQL := utils.StringsBuilder(`SELECT '`, numberColName, ` BETWEEN ' || start_id || ' AND ' || end_id || '' CMD 
FROM user_parallel_execute_chunks WHERE  task_name = '`, taskName, `' ORDER BY chunk_id`)

	_, res, err := Query(e.OracleDB, querySQL)
	if err != nil {
		return rowCount, err
	}

	// 判断数据是否存在，更新 data_diff_meta 记录，无需同步
	if len(res) == 0 {
		Logger.Warn("get oracle table rowids rows",
			zap.String("schema", sourceSchema),
			zap.String("table", sourceSchema),
			zap.String("where", "1 = 1"),
			zap.Int("rows", len(res)))

		if err = e.GormDB.Create(&DataDiffMeta{
			SourceSchemaName: strings.ToUpper(sourceSchema),
			SourceTableName:  strings.ToUpper(sourceTable),
			SourceColumnInfo: sourceColumnInfo,
			TargetColumnInfo: targetColumnInfo,
			Range:            "1 = 1",
			NumberColumn:     "",
			IsPartition:      isPartition,
		}).Error; err != nil {
			return rowCount, fmt.Errorf("gorm create table [%s.%s] data_diff_meta failed [rowids rows = 0]: %v", sourceSchema, sourceTable, err)
		}

		return rowCount, nil
	}

	var fullMetas []DataDiffMeta
	for _, r := range res {
		fullMetas = append(fullMetas, DataDiffMeta{
			SourceSchemaName: strings.ToUpper(sourceSchema),
			SourceTableName:  strings.ToUpper(sourceTable),
			SourceColumnInfo: sourceColumnInfo,
			TargetColumnInfo: targetColumnInfo,
			Range:            r["CMD"],
			NumberColumn:     numberColName,
			IsPartition:      isPartition,
		})
	}

	// 元数据库信息 batch 写入
	if err := e.GormDB.CreateInBatches(&fullMetas, insertBatchSize).Error; err != nil {
		return len(res), fmt.Errorf("gorm create table [%s.%s] data_diff_meta [batch size]failed: %v", sourceSchema, sourceTable, err)
	}

	return len(res), nil
}
