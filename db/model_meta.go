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
	"time"
)

// 全量同步元数据表
type TableFullMeta struct {
	ID               uint       `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string     `gorm:"not null;index:idx_schema_name;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string     `gorm:"not null;index:idx_schema_table;comment:'源端表名'" json:"source_table_name"`
	RowIdSQL         string     `gorm:"comment:'表 rowid 切分SQL'" json:"rowid_sql"`
	CreatedAt        *time.Time `gorm:"type:timestamp;not null;default:current_timestamp;comment:'创建时间'" json:"createdAt"`
	UpdatedAt        *time.Time `gorm:"type:timestamp;not null on update current_timestamp;default:current_timestamp;comment:'更新时间'" json:"updatedAt"`
}

// 增量同步元数据表
type TableIncrementMeta struct {
	ID               uint       `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	GlobalSCN        string     `gorm:"comment:'全局 SCN'" json:"id"`
	SourceSchemaName string     `gorm:"not null;index:unique_schema_table,unique;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string     `gorm:"not null;index:unique_schema_table,unique;comment:'源端表名'" json:"source_table_name"`
	SourceTableSCN   string     `gorm:"comment:'表同步 SCN'" json:"source_table_scn"`
	CreatedAt        *time.Time `gorm:"type:timestamp;not null;default:current_timestamp;comment:'创建时间'" json:"createdAt"`
	UpdatedAt        *time.Time `gorm:"type:timestamp;not null on update current_timestamp;default:current_timestamp;comment:'更新时间'" json:"updatedAt"`
}

func (f *TableFullMeta) GetTableFullMetaRecordCounts(engine *Engine) (int, error) {
	var count int64
	if err := engine.GormDB.Model(&TableFullMeta{}).Count(&count).Error; err != nil {
		return int(count), fmt.Errorf("meta schema table [table_full_meta] query count failed: %v", err)
	}
	return int(count), nil
}

func (f *TableIncrementMeta) GetTableIncrementMetaRecordCounts(engine *Engine) (int, error) {
	var count int64
	if err := engine.GormDB.Model(&TableIncrementMeta{}).Count(&count).Error; err != nil {
		return int(count), fmt.Errorf("meta schema table [table_increment_meta] query count failed: %v", err)
	}
	return int(count), nil
}

func (i *TableIncrementMeta) UpdateSyncMeta(engine *Engine) error {
	if err := engine.GormDB.Create(&i).Error; err != nil {
		return fmt.Errorf("gorm update sync_meta table failed: %v", err)
	}
	return nil
}
