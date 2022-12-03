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
package model

import (
	"context"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/errors"
	"gorm.io/gorm"
)

/*
	上下游字段类型映射表
*/
// 数据类型转换优先级： column > table > schema > build-in
// 自定义列转换规则 - 字段列级别
type ColumnRuleMap struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端表名'" json:"source_table_name"`
	SourceColumnName string `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端表字段列名'" json:"source_column_name"`
	SourceColumnType string `gorm:"not null;index:idx_source_col;comment:'源端表字段类型'" json:"source_column_type"`
	TargetColumnType string `gorm:"not null;index:idx_source_col;comment:'目标表字段类型'" json:"target_column_type"`
	ReverseMode      string `gorm:"not null;index:idx_reverse_mode;comment:'表结构转换模式 ReverseO2M/ReverseM2O'" json:"reverse_mode"`
	*BaseModel
}

// 自定义表转换规则 - table 级别
type TableRuleMap struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端表名'" json:"source_table_name"`
	SourceColumnType string `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端表字段类型'" json:"source_column_type"`
	TargetColumnType string `gorm:"not null;index:idx_target_col;comment:'目标表字段类型'" json:"target_column_type"`
	ReverseMode      string `gorm:"not null;index:idx_reverse_mode;comment:'表结构转换模式 ReverseO2M/ReverseM2O'" json:"reverse_mode"`
	*BaseModel
}

// 自定义库转换规则 - schema 级别
type SchemaRuleMap struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:unique_schema_col,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceColumnType string `gorm:"not null;index:unique_schema_col,unique;comment:'源端表字段类型'" json:"source_column_type"`
	TargetColumnType string `gorm:"not null;index:idx_target_col;comment:'目标表字段类型'" json:"target_column_type"`
	ReverseMode      string `gorm:"not null;index:idx_reverse_mode;comment:'表结构转换模式 ReverseO2M/ReverseM2O'" json:"reverse_mode"`
	*BaseModel
}

// 自定义字段默认值转换规则 - global 级别
type DefaultValueMap struct {
	ID                 uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceDefaultValue string `gorm:"not null;index:unique_source_default,unique;comment:'源端默认值'" json:"source_default_value"`
	TargetDefaultValue string `gorm:"not null;index:idx_target_default;comment:'目标默认值'" json:"target_default_value"`
	ReverseMode        string `gorm:"not null;index:idx_reverse_mode;comment:'表结构转换模式 ReverseO2M/ReverseM2O'" json:"reverse_mode"`
	*BaseModel
}

type ReverseRuleMap struct {
	*ColumnRuleMap
	*TableRuleMap
	*SchemaRuleMap
	*DefaultValueMap
}

func NewReverseModel(gormDB *gorm.DB) *ReverseRuleMap {
	return &ReverseRuleMap{
		ColumnRuleMap: &ColumnRuleMap{BaseModel: &BaseModel{
			MetaDB: WrapGormDB(gormDB),
		}},
		TableRuleMap: &TableRuleMap{BaseModel: &BaseModel{
			MetaDB: WrapGormDB(gormDB),
		}},
		SchemaRuleMap: &SchemaRuleMap{BaseModel: &BaseModel{
			MetaDB: WrapGormDB(gormDB),
		}},
		DefaultValueMap: &DefaultValueMap{BaseModel: &BaseModel{
			MetaDB: WrapGormDB(gormDB),
		}},
	}
}

func (rw *ColumnRuleMap) Create(ctx context.Context, createS interface{}) error {
	if err := rw.DB(ctx).Create(createS.(*ColumnRuleMap)).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *ColumnRuleMap) Detail(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*ColumnRuleMap)
	var columnRuleMap []ColumnRuleMap
	if err := rw.DB(ctx).Where("UPPER(source_schema_name) = ? AND UPPER(source_table_name) = ? AND UPPER(source_column_name) = ? AND UPPER(reverse_mode) = ?",
		common.StringUPPER(ds.SourceSchemaName), common.StringUPPER(ds.SourceTableName),
		common.StringUPPER(ds.SourceColumnName), common.StringUPPER(ds.ReverseMode)).Find(&columnRuleMap).Error; err != nil {
		return columnRuleMap, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}

	return columnRuleMap, nil
}

func (rw *TableRuleMap) Create(ctx context.Context, createS interface{}) error {
	if err := rw.DB(ctx).Create(createS.(*TableRuleMap)).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *TableRuleMap) Detail(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*TableRuleMap)
	var tableRuleMap []TableRuleMap
	if err := rw.DB(ctx).Where("UPPER(source_schema_name) = ? AND UPPER(source_table_name) = ? AND UPPER(reverse_mode) = ?", common.StringUPPER(ds.SourceSchemaName), common.StringUPPER(ds.SourceTableName), common.StringUPPER(ds.ReverseMode)).Find(&tableRuleMap).Error; err != nil {
		return tableRuleMap, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}

	return tableRuleMap, nil
}

func (rw *SchemaRuleMap) Create(ctx context.Context, createS interface{}) error {
	if err := rw.DB(ctx).Create(createS.(SchemaRuleMap)).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *SchemaRuleMap) Detail(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*SchemaRuleMap)
	var schemaRuleMap []SchemaRuleMap
	if err := rw.DB(ctx).Where("UPPER(source_schema_name) = ? AND UPPER(reverse_mode) = ?",
		common.StringUPPER(ds.SourceSchemaName), common.StringUPPER(ds.ReverseMode)).Find(&schemaRuleMap).Error; err != nil {
		return schemaRuleMap, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}

	return schemaRuleMap, nil
}

func (rw *DefaultValueMap) Create(ctx context.Context, createS interface{}) error {
	if err := rw.DB(ctx).Create(createS.(*DefaultValueMap)).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *DefaultValueMap) Detail(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*DefaultValueMap)
	var defaultRuleMap []DefaultValueMap
	if err := rw.DB(ctx).Where("UPPER(reverse_mode) = ?", common.StringUPPER(ds.ReverseMode)).Find(&defaultRuleMap).Error; err != nil {
		return defaultRuleMap, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}

	return defaultRuleMap, nil
}

func (rw *DefaultValueMap) RowsAffected(ctx context.Context, defaultVal *DefaultValueMap) int64 {
	u := &DefaultValueMap{}
	return rw.DB(ctx).Where(defaultVal).Find(&u).RowsAffected
}
