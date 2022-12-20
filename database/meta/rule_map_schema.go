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
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/errors"
)

// 自定义库转换规则 - schema 级别
type SchemaRuleMap struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:unique_schema_col,unique;comment:'源端库 schema'" json:"source_schema_name"`
	SourceColumnType string `gorm:"not null;index:unique_schema_col,unique;comment:'源端表字段类型'" json:"source_column_type"`
	TargetColumnType string `gorm:"not null;index:idx_target_col;comment:'目标表字段类型'" json:"target_column_type"`
	ReverseMode      string `gorm:"not null;index:idx_reverse_mode;comment:'表结构转换模式 ReverseO2M/ReverseM2O'" json:"reverse_mode"`
	*BaseModel
}

func NewSchemaRuleMapModel(m *Meta) *SchemaRuleMap {
	return &SchemaRuleMap{BaseModel: &BaseModel{
		Meta: m,
	}}
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
