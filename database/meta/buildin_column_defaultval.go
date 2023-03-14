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
package meta

import (
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"gorm.io/gorm"
)

// 自定义字段默认值转换规则 - column 级别
type BuildinColumnDefaultval struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS       string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT       string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS   string `gorm:"type:varchar(200);index:idx_dbtype_st_map,unique;comment:'源数据库名'" json:"schema_name_s"`
	TableNameS    string `gorm:"type:varchar(200);index:idx_dbtype_st_map,unique;comment:'源数据库表名'" json:"table_name_s"`
	ColumnNameS   string `gorm:"type:varchar(200);index:idx_dbtype_st_map,unique;comment:'源数据库表字段名'" json:"column_name_s"`
	DefaultValueS string `gorm:"type:varchar(200);comment:'源端字段默认值'" json:"default_value_s"`
	DefaultValueT string `gorm:"type:varchar(200);not null;comment:'目标默认值'" json:"default_value_t"`
	*BaseModel
}

func NewBuildinColumnDefaultvalModel(m *Meta) *BuildinColumnDefaultval {
	return &BuildinColumnDefaultval{BaseModel: &BaseModel{
		Meta: m,
	}}
}

func (rw *BuildinColumnDefaultval) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [BuildinColumnDefaultval] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *BuildinColumnDefaultval) CreateColumnDefaultVal(ctx context.Context, createS *BuildinColumnDefaultval) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Create(createS).Error; err != nil {
		return fmt.Errorf("create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *BuildinColumnDefaultval) DetailColumnDefaultVal(ctx context.Context, detailS *BuildinColumnDefaultval) ([]BuildinColumnDefaultval, error) {
	var defaultRuleMap []BuildinColumnDefaultval

	table, err := rw.ParseSchemaTable()
	if err != nil {
		return defaultRuleMap, err
	}
	if err := rw.DB(ctx).Where("UPPER(db_type_s) = ? AND UPPER(db_type_t) = ? AND UPPER(schema_name_s) = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS)).Find(&defaultRuleMap).Error; err != nil {
		return defaultRuleMap, fmt.Errorf("detail table [%s] record failed: %v", table, err)
	}

	return defaultRuleMap, nil
}
