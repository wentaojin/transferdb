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

// 表错误详情
// 用于 reverse 和 check 模式
type TableErrorDetail struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:idx_schema_table_mode;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:idx_schema_table_mode;comment:'源端表名'" json:"source_table_name"`
	RunMode          string `gorm:"not null;index:idx_schema_table_mode;comment:'运行模式'" json:"run_mode"`
	InfoSources      string `gorm:"not null;comment:'信息来源'" json:"info_sources"`
	RunStatus        string `gorm:"not null;comment:'运行状态'" json:"run_status"`
	InfoDetail       string `gorm:"not null;comment:'信息详情'" json:"info_detail"`
	ErrorDetail      string `gorm:"not null;comment:'错误详情'" json:"error_detail"`
	*BaseModel
}

func NewTableErrorDetailModel(gormDB *gorm.DB) *TableErrorDetail {
	return &TableErrorDetail{
		BaseModel: &BaseModel{
			MetaDB: WrapGormDB(gormDB),
		},
	}
}

func (rw *TableErrorDetail) Create(ctx context.Context, createS interface{}) error {
	if err := rw.DB(ctx).Create(createS.(*TableErrorDetail)).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *TableErrorDetail) Detail(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*TableErrorDetail)
	var tableErrDetails []TableErrorDetail
	if err := rw.DB(ctx).Where("UPPER(source_schema_name) = ? AND run_mode = ?",
		common.StringUPPER(ds.SourceSchemaName), ds.RunMode).Find(&tableErrDetails).Error; err != nil {
		return tableErrDetails, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}

	return tableErrDetails, nil
}

func (rw *TableErrorDetail) CountsBySchema(ctx context.Context, tableErrDetail interface{}) (int64, error) {
	ds := tableErrDetail.(*TableErrorDetail)
	var totals int64
	if err := rw.DB(ctx).Model(&TableErrorDetail{}).
		Where(`source_schema_name = ? AND run_mode = ?`, common.StringUPPER(ds.SourceSchemaName),
			ds.RunMode).
		Count(&totals).Error; err != nil {
		return totals, err
	}
	return totals, nil
}

func (rw *TableErrorDetail) Counts(ctx context.Context, tableErrDetail interface{}) (int64, error) {
	ds := tableErrDetail.(*TableErrorDetail)
	var totals int64
	if err := rw.DB(ctx).Model(&TableErrorDetail{}).
		Where(`source_schema_name = ? AND source_table_name = ? AND run_mode = ?`, common.StringUPPER(ds.SourceSchemaName),
			common.StringUPPER(ds.SourceTableName),
			ds.RunMode).
		Count(&totals).Error; err != nil {
		return totals, err
	}
	return totals, nil
}
