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
	"strings"
	"time"

	"github.com/WentaoJin/transferdb/util"
)

// 同步元数据表
type TableMeta struct {
	ID               uint       `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string     `gorm:"not null;index:idx_schema_table;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string     `gorm:"not null;index:idx_schema_table;comment:'源端表名'" json:"source_table_name"`
	FullGlobalSCN    int        `gorm:"comment:'全量全局 SCN'" json:"full_global_scn"`
	FullSplitTimes   int        `gorm:"comment:'全量任务切分 SQL 次数'" json:"full_split_times"`
	CreatedAt        *time.Time `gorm:"type:timestamp;not null;default:current_timestamp;comment:'创建时间'" json:"createdAt"`
	UpdatedAt        *time.Time `gorm:"type:timestamp;not null on update current_timestamp;default:current_timestamp;comment:'更新时间'" json:"updatedAt"`
}

// 全量同步元数据表
type TableFullMeta struct {
	ID               uint       `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string     `gorm:"not null;index:idx_schema_table_rowid;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string     `gorm:"not null;index:idx_schema_table_rowid;comment:'源端表名'" json:"source_table_name"`
	GlobalSCN        int        `gorm:"comment:'全局 SCN'" json:"global_scn"`
	RowidSQL         string     `gorm:"not null;index:idx_schema_table_rowid;comment:'表 rowid 切分SQL'" json:"rowid_sql"`
	IsPartition      string     `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	CreatedAt        *time.Time `gorm:"type:timestamp;not null;default:current_timestamp;comment:'创建时间'" json:"createdAt"`
	UpdatedAt        *time.Time `gorm:"type:timestamp;not null on update current_timestamp;default:current_timestamp;comment:'更新时间'" json:"updatedAt"`
}

// 增量同步元数据表
type TableIncrementMeta struct {
	ID               uint       `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	GlobalSCN        int        `gorm:"comment:'全局 SCN'" json:"global_scn"`
	SourceSchemaName string     `gorm:"not null;index:unique_schema_table,unique;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string     `gorm:"not null;index:unique_schema_table,unique;comment:'源端表名'" json:"source_table_name"`
	SourceTableSCN   int        `gorm:"comment:'表同步 SCN'" json:"source_table_scn"`
	CreatedAt        *time.Time `gorm:"type:timestamp;not null;default:current_timestamp;comment:'创建时间'" json:"createdAt"`
	UpdatedAt        *time.Time `gorm:"type:timestamp;not null on update current_timestamp;default:current_timestamp;comment:'更新时间'" json:"updatedAt"`
}

func (f *TableFullMeta) GetTableFullMetaRecordCounts(schemaName, tableName string, engine *Engine) (int, error) {
	var count int64
	if err := engine.GormDB.Model(&TableFullMeta{}).
		Where("upper(source_schema_name) = ? and upper(source_table_name) = ?",
			strings.ToUpper(schemaName),
			strings.ToUpper(tableName)).
		Count(&count).Error; err != nil {
		return int(count), fmt.Errorf("meta schema table [table_full_meta] query record count failed: %v", err)
	}
	return int(count), nil
}

func (i *TableIncrementMeta) GetTableIncrementMetaRowCounts(engine *Engine) (int, error) {
	var count int64
	if err := engine.GormDB.Model(&TableIncrementMeta{}).
		Count(&count).Error; err != nil {
		return int(count), fmt.Errorf("meta schema table [table_increment_meta] query row count failed: %v", err)
	}
	return int(count), nil
}

func (e *Engine) UpdateTableIncrementMetaALLSCNRecord(sourceSchemaName, sourceTableName, operationType string, globalSCN, sourceTableSCN int) error {
	if operationType == util.DropTableOperation {
		if err := e.GormDB.Where("upper(source_schema_name) = ? and upper(source_table_name) = ?",
			strings.ToUpper(sourceSchemaName),
			strings.ToUpper(sourceTableName)).
			Delete(&TableIncrementMeta{}).Error; err != nil {
			return err
		}
		return nil
	}
	if err := e.GormDB.Model(TableIncrementMeta{}).Where("upper(source_schema_name) = ? and upper(source_table_name) = ?",
		strings.ToUpper(sourceSchemaName),
		strings.ToUpper(sourceTableName)).
		Updates(TableIncrementMeta{GlobalSCN: globalSCN, SourceTableSCN: sourceTableSCN}).Error; err != nil {
		return err
	}
	return nil
}

func (e *Engine) UpdateSingleTableIncrementMetaSCNByCurrentRedo(
	sourceSchemaName string, maxLogFileSCN, logFileStartSCN, logFileEndSCN int) error {
	var logFileSCN int
	if logFileEndSCN >= maxLogFileSCN {
		logFileSCN = logFileStartSCN
	} else {
		logFileSCN = logFileEndSCN
	}

	var tableIncrMeta []TableIncrementMeta
	if err := e.GormDB.Model(TableIncrementMeta{}).Where(
		"upper(source_schema_name) = ?",
		strings.ToUpper(sourceSchemaName)).Find(&tableIncrMeta).Error; err != nil {
		return err
	}

	for _, table := range tableIncrMeta {
		if table.GlobalSCN < logFileSCN || table.GlobalSCN > logFileSCN {
			if err := e.GormDB.Model(TableIncrementMeta{}).Where(
				"upper(source_schema_name) = ? and upper(source_table_name) = ?",
				strings.ToUpper(sourceSchemaName),
				strings.ToUpper(table.SourceTableName)).
				Updates(TableIncrementMeta{
					GlobalSCN: logFileSCN,
				}).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) UpdateSingleTableIncrementMetaSCNByNonCurrentRedo(
	sourceSchemaName string, logFileEndSCN int,
	transferTableSlice []string) error {
	for _, table := range transferTableSlice {
		if err := e.GormDB.Model(TableIncrementMeta{}).Where(
			"upper(source_schema_name) = ? and upper(source_table_name) = ?",
			strings.ToUpper(sourceSchemaName),
			strings.ToUpper(table)).
			Updates(TableIncrementMeta{
				GlobalSCN:      logFileEndSCN,
				SourceTableSCN: logFileEndSCN,
			}).Error; err != nil {
			return err
		}
	}
	return nil
}
