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
)

/*
	上下游字段类型映射表
*/
// 数据类型转换优先级： column > table > schema > build-in
// 自定义列转换规则 - 字段列级别
type ColumnRuleMap struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:unique_schema_col,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:unique_schema_col,unique;comment:'源端表名'" json:"source_table_name"`
	SourceColumnName string `gorm:"not null;index:unique_schema_col,unique;comment:'源端表字段列名'" json:"source_column_name"`
	SourceColumnType string `gorm:"not null;index:idx_source_col;comment:'源端表字段类型'" json:"source_column_type"`
	TargetColumnType string `gorm:"not null;index:idx_source_col;comment:'目标表字段类型'" json:"target_column_type"`
	BaseModel
}

// 自定义表转换规则 - table 级别
type TableRuleMap struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端表名'" json:"source_table_name"`
	SourceColumnType string `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端表字段类型'" json:"source_column_type"`
	TargetColumnType string `gorm:"not null;index:idx_target_col;comment:'目标表字段类型'" json:"target_column_type"`
	BaseModel
}

// 自定义库转换规则 - schema 级别
type SchemaRuleMap struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:unique_schema_col,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceColumnType string `gorm:"not null;index:unique_schema_col,unique;comment:'源端表字段类型'" json:"source_column_type"`
	TargetColumnType string `gorm:"not null;index:idx_target_col;comment:'目标表字段类型'" json:"target_column_type"`
	BaseModel
}

// 自定义字段默认值转换规则 - global 级别
type DefaultValueMap struct {
	ID                 uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceDefaultValue string `gorm:"not null;index:unique_source_default,unique;comment:'源端默认值'" json:"source_default_value"`
	TargetDefaultValue string `gorm:"not null;index:idx_target_default;comment:'目标默认值'" json:"target_default_value"`
	BaseModel
}

func (e *Engine) InitDefaultValueMap() error {
	var counts int64
	if err := e.GormDB.Model(&DefaultValueMap{}).Where(
		"upper(source_default_value) = ? and upper(target_default_value) = ?",
		utils.DefaultValueSysdate, utils.DefaultValueSysdateMap).Or(
		"lower(source_default_value) = ? and lower(target_default_value) = ?",
		strings.ToLower(utils.DefaultValueSysdate), strings.ToLower(utils.DefaultValueSysdateMap)).Count(&counts).Error; err != nil {
		return err
	}

	if counts == 0 {
		if err := e.GormDB.Create(&DefaultValueMap{
			SourceDefaultValue: utils.DefaultValueSysdate,
			TargetDefaultValue: utils.DefaultValueSysdateMap,
			BaseModel: BaseModel{
				CreatedAt: getCurrentTime(),
				UpdatedAt: getCurrentTime(),
			},
		}).Error; err != nil {
			return err
		}
	}
	if counts > 1 {
		return fmt.Errorf("panic, mysql get multiple record, counts: %d, query condition: source_default_value [%s],target_default_value [%s]", counts, utils.DefaultValueSysdate, utils.DefaultValueSysdateMap)
	}
	return nil
}

func (e *Engine) GetDefaultValueMap() ([]DefaultValueMap, error) {
	var c []DefaultValueMap
	if err := e.GormDB.Find(&c).Error; err != nil {
		return c, fmt.Errorf("get custom default value rule map failed: %v", err)
	}
	return c, nil
}

func (e *Engine) GetColumnRuleMap(schemaName, tableName string) ([]ColumnRuleMap, error) {
	var c []ColumnRuleMap
	if err := e.GormDB.Where("upper(source_schema_name) = ? AND upper(source_table_name) = ?",
		strings.ToUpper(schemaName), strings.ToUpper(tableName)).Find(&c).Error; err != nil {
		return c, fmt.Errorf("get custom column data type map by schema [%s] failed: %v", schemaName, err)
	}
	return c, nil
}

func (e *Engine) GetTableRuleMap(schemaName, tableName string) ([]TableRuleMap, error) {
	var c []TableRuleMap
	if err := e.GormDB.Where("upper(source_schema_name) = ? AND upper(source_table_name) = ?",
		strings.ToUpper(schemaName), strings.ToUpper(tableName)).Find(&c).Error; err != nil {
		return c, fmt.Errorf("get custom table data type map by schema [%s] table failed: %v", schemaName, err)
	}
	return c, nil
}

func (e *Engine) GetSchemaRuleMap(schemaName string) ([]SchemaRuleMap, error) {
	var c []SchemaRuleMap
	if err := e.GormDB.Where("upper(source_schema_name) = ? ", strings.ToUpper(schemaName)).Find(&c).Error; err != nil {
		return c, fmt.Errorf("get custom schema data type map by schema [%s] failed: %v", schemaName, err)
	}
	return c, nil
}

func (c *ColumnRuleMap) AdjustColumnDataType(tableName, columnName string) string {
	var colType string
	if strings.ToUpper(tableName) == strings.ToUpper(c.SourceTableName) && strings.ToUpper(columnName) == strings.ToUpper(c.SourceColumnName) {
		if c.TargetColumnType != "" {
			colType = c.TargetColumnType
		} else {
			colType = c.SourceColumnType
		}
	}
	return colType
}

func (c *TableRuleMap) AdjustTableDataType(tableName string) string {
	var colType string
	if strings.ToUpper(tableName) == strings.ToUpper(c.SourceTableName) {
		if c.TargetColumnType != "" {
			colType = c.TargetColumnType
		} else {
			colType = c.SourceColumnType
		}
	}
	return colType
}

func (c *SchemaRuleMap) AdjustSchemaDataType() string {
	var colType string
	if c.TargetColumnType != "" {
		colType = c.TargetColumnType
	} else {
		colType = c.SourceColumnType
	}
	return colType
}
