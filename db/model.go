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
)

/*
	上下游字段类型映射表
*/
// 自定义表结构名称转换规则 - table 级别
// 如果同步表未单独配置表结构名称转换规则 ，则采用源表默认
type CustomTableNameMap struct {
	ID               uint       `gorm:"primary_key;comment:'自增编号'" json:"id"`
	SourceSchemaName string     `gorm:"not null;index:unique_schema_table,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceTableName  string     `gorm:"not null;index:unique_schema_table,unique;comment:'源端表名'" json:"source_table_name"`
	TargetTableName  string     `gorm:"not null;index:unique_schema_table,unique;comment:'目标表名'" json:"target_table_name"`
	CreatedAt        *time.Time `gorm:"type:timestamp;not null;default:current_timestamp;comment:'创建时间'" json:"createdAt"`
	UpdatedAt        *time.Time `gorm:"type:timestamp;not null on update current_timestamp;default:current_timestamp;comment:'更新时间'" json:"updatedAt"`
}

// 自定义表结构字段类型转换规则 - table 级别
// 如果同步表未单独配置 table 级别字段类型映射规则，并且未单独配置 schema 级别字段类型映射规则，则采用程序内置规则转换
// 优先级： table > schema > internal
type CustomTableColumnTypeMap struct {
	ID               uint       `gorm:"primary_key;comment:'自增编号'" json:"id"`
	SourceSchemaName string     `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceTableName  string     `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端表名'" json:"source_table_name"`
	SourceColumnType string     `gorm:"not null;index:unique_schema_table_col,unique;comment:'源端表字段类型'" json:"source_column_type"`
	TargetColumnType string     `gorm:"not null;index:unique_schema_table_col,unique;comment:'目标表字段类型'" json:"target_column_type"`
	CreatedAt        *time.Time `gorm:"type:timestamp;not null;default:current_timestamp;comment:'创建时间'" json:"createdAt"`
	UpdatedAt        *time.Time `gorm:"type:timestamp;not null on update current_timestamp;default:current_timestamp;comment:'更新时间'" json:"updatedAt"`
}

// 自定义表结构字段类型转换规则 - schema 级别
type CustomSchemaColumnTypeMap struct {
	ID               uint       `gorm:"primary_key;comment:'自增编号'" json:"id"`
	SourceSchemaName string     `gorm:"not null;index:unique_schema_col,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceColumnType string     `gorm:"not null;index:unique_schema_col,unique;comment:'源端表字段类型'" json:"source_column_type"`
	TargetColumnType string     `gorm:"not null;index:unique_schema_col,unique;comment:'目标表字段类型'" json:"target_column_type"`
	CreatedAt        *time.Time `gorm:"type:timestamp;not null;default:current_timestamp;comment:'创建时间'" json:"createdAt"`
	UpdatedAt        *time.Time `gorm:"type:timestamp;not null on update current_timestamp;default:current_timestamp;comment:'更新时间'" json:"updatedAt"`
}

func (e *Engine) GetCustomTableNameMap(schemaName string) ([]CustomTableNameMap, error) {
	var c []CustomTableNameMap

	if err := e.GormDB.Where("source_schema_name = ?", schemaName).Find(&c).Error; err != nil {
		return c, fmt.Errorf("get custom table column name map by schema [%s] failed: %v", schemaName, err)
	}

	return c, nil
}

func (e *Engine) GetCustomTableColumnTypeMap(schemaName string) ([]CustomTableColumnTypeMap, error) {
	var c []CustomTableColumnTypeMap
	if err := e.GormDB.Where("source_schema_name = ?", schemaName).Find(&c).Error; err != nil {
		return c, fmt.Errorf("get custom table column type map by schema [%s] table failed: %v", schemaName, err)
	}
	return c, nil
}

func (e *Engine) GetCustomSchemaColumnTypeMap(schemaName string) ([]CustomSchemaColumnTypeMap, error) {
	var c []CustomSchemaColumnTypeMap
	if err := e.GormDB.Where("source_schema_name = ? ", schemaName).Find(&c).Error; err != nil {
		return c, fmt.Errorf("get custom schema column type map by schema [%s] failed: %v", schemaName, err)
	}
	return c, nil
}

func (c *CustomSchemaColumnTypeMap) GetCustomSchemaColumnType() string {
	var colType string
	if c.TargetColumnType != "" {
		colType = c.TargetColumnType
	} else {
		colType = c.SourceColumnType
	}
	return colType
}

func (c *CustomTableColumnTypeMap) GetCustomTableColumnType(tableName string) string {
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
