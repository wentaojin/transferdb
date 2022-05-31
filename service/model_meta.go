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
	"strings"

	"github.com/wentaojin/transferdb/utils"

	"gorm.io/gorm"
)

// 同步元数据表
type WaitSyncMeta struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:idx_schema_table_mode,unique;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:idx_schema_table_mode,unique;comment:'源端表名'" json:"source_table_name"`
	SyncMode         string `gorm:"not null;index:idx_schema_table_mode,unique;comment:'同步模式'" json:"sync_mode"`
	FullGlobalSCN    uint64 `gorm:"comment:'全量全局 SCN'" json:"full_global_scn"`
	FullSplitTimes   int    `gorm:"comment:'全量任务切分 SQL 次数'" json:"full_split_times"`
	IsPartition      string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	BaseModel
}

// 全量同步元数据表
type FullSyncMeta struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:idx_schema_table_rowid;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:idx_schema_table_rowid;comment:'源端表名'" json:"source_table_name"`
	GlobalSCN        uint64 `gorm:"comment:'全局 SCN'" json:"global_scn"`
	SourceSQL        string `gorm:"type:text;comment:'源端 sql'" json:"source_sql"`
	RowidSQL         string `gorm:"type:varchar(300);not null;index:idx_schema_table_rowid;comment:'表 rowid 切分SQL'" json:"rowid_sql"`
	IsPartition      string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	CSVFile          string `gorm:"type:varchar(300);comment:'csv 文件名'" json:"csv_file"`
	BaseModel
}

// 增量同步元数据表
type IncrementSyncMeta struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	GlobalSCN        uint64 `gorm:"comment:'全局 SCN'" json:"global_scn"`
	SourceSchemaName string `gorm:"not null;index:unique_schema_table,unique;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:unique_schema_table,unique;comment:'源端表名'" json:"source_table_name"`
	SourceTableSCN   uint64 `gorm:"comment:'表同步 SCN'" json:"source_table_scn"`
	IsPartition      string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	BaseModel
}

func (f *FullSyncMeta) GetFullSyncMetaTableName(schemaName string, engine *Engine) ([]string, error) {
	var tables []string
	if err := engine.GormDB.Model(&FullSyncMeta{}).
		Where("source_schema_name = ?",
			strings.ToUpper(schemaName)).Distinct().Pluck("source_table_name", &tables).Error; err != nil {
		return tables, fmt.Errorf("meta schema table [full_sync_meta] query source_table_name record failed: %v", err)
	}
	return tables, nil
}

func (f *IncrementSyncMeta) GetIncrementSyncMetaRecordCounts(schemaName, tableName string, engine *Engine) (int, error) {
	var count int64
	if err := engine.GormDB.Model(&IncrementSyncMeta{}).
		Where("source_schema_name = ? and source_table_name = ?",
			strings.ToUpper(schemaName),
			strings.ToUpper(tableName)).
		Count(&count).Error; err != nil {
		return int(count), fmt.Errorf("meta schema table [increment_sync_meta] query record count failed: %v", err)
	}
	return int(count), nil
}

func (e *Engine) UpdateTableIncrementMetaALLSCNRecord(sourceSchemaName, sourceTableName, operationType string, globalSCN, sourceTableSCN uint64) error {
	if operationType == utils.DropTableOperation {
		if err := e.GormDB.Transaction(func(tx *gorm.DB) error {
			if err := tx.Where("source_schema_name = ? and source_table_name = ?",
				strings.ToUpper(sourceSchemaName),
				strings.ToUpper(sourceTableName)).
				Delete(&IncrementSyncMeta{}).Error; err != nil {
				return err
			}

			if err := tx.Where("source_schema_name = ? and source_table_name = ?",
				strings.ToUpper(sourceSchemaName),
				strings.ToUpper(sourceTableName)).
				Delete(&WaitSyncMeta{}).Error; err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}
	if err := e.GormDB.Model(&IncrementSyncMeta{}).Where("source_schema_name = ? and source_table_name = ?",
		strings.ToUpper(sourceSchemaName),
		strings.ToUpper(sourceTableName)).
		Updates(IncrementSyncMeta{GlobalSCN: globalSCN, SourceTableSCN: sourceTableSCN}).Error; err != nil {
		return err
	}
	return nil
}

func (e *Engine) UpdateSingleTableIncrementMetaSCNByCurrentRedo(
	sourceSchemaName string, lastRedoLogMaxSCN, logFileStartSCN, logFileEndSCN uint64) error {
	var logFileSCN uint64
	if logFileEndSCN >= lastRedoLogMaxSCN {
		logFileSCN = logFileStartSCN
	} else {
		logFileSCN = logFileEndSCN
	}

	var tableIncrMeta []IncrementSyncMeta
	if err := e.GormDB.Model(IncrementSyncMeta{}).Where(
		"source_schema_name = ?",
		strings.ToUpper(sourceSchemaName)).Find(&tableIncrMeta).Error; err != nil {
		return err
	}

	for _, table := range tableIncrMeta {
		if table.GlobalSCN < logFileSCN {
			if err := e.GormDB.Model(&IncrementSyncMeta{}).Where(
				"source_schema_name = ? and source_table_name = ?",
				strings.ToUpper(sourceSchemaName),
				strings.ToUpper(table.SourceTableName)).
				Updates(IncrementSyncMeta{
					GlobalSCN: logFileSCN,
				}).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) UpdateSingleTableIncrementMetaSCNByNonCurrentRedo(
	sourceSchemaName string, lastRedoLogMaxSCN, logFileStartSCN, logFileEndSCN uint64, transferTableSlice []string) error {
	var logFileSCN uint64
	if logFileEndSCN >= lastRedoLogMaxSCN {
		logFileSCN = logFileStartSCN
	} else {
		logFileSCN = logFileEndSCN
	}

	for _, table := range transferTableSlice {
		if err := e.GormDB.Model(&IncrementSyncMeta{}).Where(
			"source_schema_name = ? and source_table_name = ?",
			strings.ToUpper(sourceSchemaName),
			strings.ToUpper(table)).
			Updates(IncrementSyncMeta{
				GlobalSCN: logFileSCN,
			}).Error; err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) UpdateSingleTableIncrementMetaSCNByArchivedLog(
	sourceSchemaName string, logFileEndSCN uint64, transferTableSlice []string) error {
	for _, table := range transferTableSlice {
		if err := e.GormDB.Model(&IncrementSyncMeta{}).Where(
			"source_schema_name = ? and source_table_name = ?",
			strings.ToUpper(sourceSchemaName),
			strings.ToUpper(table)).
			Updates(IncrementSyncMeta{
				GlobalSCN:      logFileEndSCN,
				SourceTableSCN: logFileEndSCN,
			}).Error; err != nil {
			return err
		}
	}
	return nil
}
