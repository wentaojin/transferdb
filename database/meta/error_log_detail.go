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

type ErrorLogDetail struct {
	ID          uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS     string `gorm:"type:varchar(30);index:idx_dbtype_st_map;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT     string `gorm:"type:varchar(30);index:idx_dbtype_st_map;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS  string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map;comment:'源端表名'" json:"table_name_s"`
	SchemaNameT string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameT  string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map;comment:'目标端表名'" json:"table_name_t"`
	TaskMode    string `gorm:"type:varchar(30);not null;index:idx_dbtype_st_map;comment:'任务模式'" json:"task_mode"`
	TaskStatus  string `gorm:"type:varchar(30);not null;comment:'任务状态'" json:"task_status"`
	SourceDDL   string `gorm:"type:longtext;not null;comment:'源端原始 DDL'" json:"source_ddl"`
	TargetDDL   string `gorm:"type:longtext;not null;comment:'目标端转换 DDL'" json:"target_ddl"`
	InfoDetail  string `gorm:"type:longtext;not null;comment:'信息详情'" json:"info_detail"`
	ErrorDetail string `gorm:"type:longtext;not null;comment:'错误详情'" json:"error_detail"`
	*BaseModel
}

func NewErrorLogDetailModel(m *Meta) *ErrorLogDetail {
	return &ErrorLogDetail{
		BaseModel: &BaseModel{
			Meta: m,
		},
	}
}

func (rw *ErrorLogDetail) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [ErrorLogDetail] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *ErrorLogDetail) CreateErrorLog(ctx context.Context, createS *ErrorLogDetail) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Create(createS).Error; err != nil {
		return fmt.Errorf("create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *ErrorLogDetail) DetailErrorLog(ctx context.Context, detailS *ErrorLogDetail) ([]ErrorLogDetail, error) {
	var tableErrDetails []ErrorLogDetail
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return tableErrDetails, err
	}
	if err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND UPPER(schema_name_s) = ? AND task_mode = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS),
		detailS.TaskMode).Find(&tableErrDetails).Error; err != nil {
		return tableErrDetails, fmt.Errorf("detail table [%s] record failed: %v", table, err)
	}

	return tableErrDetails, nil
}

func (rw *ErrorLogDetail) CountsErrorLogBySchema(ctx context.Context, detailS *ErrorLogDetail) (int64, error) {
	var totals int64
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return totals, err
	}
	if err = rw.DB(ctx).Model(&ErrorLogDetail{}).
		Where(`db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND task_mode = ?`,
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
			detailS.TaskMode).
		Count(&totals).Error; err != nil {
		return totals, fmt.Errorf("get table [%s] counts failed: %v", table, err)
	}
	return totals, nil
}
