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
	"strings"
)

// 表错误详情
// 用于 reverse 和 check 模式
type TableErrorDetail struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:idx_schema_table_mode;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:idx_schema_table_mode;comment:'源端表名'" json:"source_table_name"`
	RunMode          string `gorm:"not null;index:idx_schema_table_mode;comment:'运行模式'" json:"run_mode"`
	InfoSources      string `gorm:"not null;comment:'信息来源'" json:"info_sources"`
	RunStatus        string `gorm:"not null;comment:'运行状态'" json:"run_status"`
	Detail           string `gorm:"not null;comment:'信息详情'" json:"detail"`
	Error            string `gorm:"not null;comment:'错误详情'" json:"error"`
	BaseModel
}

func (e *Engine) GetTableErrorDetailCountByMode(schemaName, runMode string) (int64, error) {
	var totals int64
	if err := e.GormDB.Model(&TableErrorDetail{}).
		Where(`source_schema_name = ? AND run_mode = ?`, strings.ToUpper(schemaName), runMode).
		Count(&totals).Error; err != nil {
		return totals, err
	}
	return totals, nil
}

func (e *Engine) GetTableErrorDetailCountBySources(schemaName, runMode, infoSources string) (int64, error) {
	var totals int64
	if err := e.GormDB.Model(&TableErrorDetail{}).
		Where(`source_schema_name = ? AND run_mode = ? AND info_sources = ?`, strings.ToUpper(schemaName), runMode, infoSources).
		Count(&totals).Error; err != nil {
		return totals, err
	}
	return totals, nil
}

func (e *Engine) GetTableErrorDetailDistinctCountByMode(schemaName, runMode string) (int64, error) {
	var totals int64
	if err := e.GormDB.Model(&TableErrorDetail{}).Distinct("source_table_name").
		Where(`source_schema_name = ? AND run_mode = ?`, strings.ToUpper(schemaName), runMode).
		Count(&totals).Error; err != nil {
		return totals, err
	}
	return totals, nil
}

func (e *Engine) GetTableErrorDetailCountByTableMode(schemaName, tableName, runMode string) (int64, error) {
	var totals int64
	if err := e.GormDB.Model(&TableErrorDetail{}).
		Where(`source_schema_name = ? AND source_table_name = ? AND run_mode = ?`, strings.ToUpper(schemaName), strings.ToUpper(tableName), runMode).
		Count(&totals).Error; err != nil {
		return totals, err
	}
	return totals, nil
}
