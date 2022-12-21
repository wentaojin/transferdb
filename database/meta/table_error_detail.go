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

// 表错误详情
// 用于 reverse 和 check 模式
type ErrorLogDetail struct {
	ID          uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS     string `gorm:"type:varchar(15);index:idx_dbtype_st_map;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT     string `gorm:"type:varchar(15);index:idx_dbtype_st_map;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS string `gorm:"not null;index:idx_dbtype_st_map;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS  string `gorm:"not null;index:idx_dbtype_st_map;comment:'源端表名'" json:"table_name_s"`
	RunMode     string `gorm:"not null;index:idx_dbtype_st_map;comment:'运行模式'" json:"run_mode"`
	RunStatus   string `gorm:"not null;comment:'运行状态'" json:"run_status"`
	InfoDetail  string `gorm:"not null;comment:'信息详情'" json:"info_detail"`
	ErrorDetail string `gorm:"not null;comment:'错误详情'" json:"error_detail"`
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
	if err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND UPPER(schema_name_s) = ? AND run_mode = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS),
		detailS.RunMode).Find(&tableErrDetails).Error; err != nil {
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
		Where(`db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND run_mode = ?`,
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
			detailS.RunMode).
		Count(&totals).Error; err != nil {
		return totals, fmt.Errorf("get table [%s] counts failed: %v", table, err)
	}
	return totals, nil
}

func (rw *ErrorLogDetail) CountsErrorLogBySchemaTable(ctx context.Context, detailS *ErrorLogDetail) (int64, error) {
	var totals int64
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return totals, err
	}
	if err = rw.DB(ctx).Model(&ErrorLogDetail{}).
		Where(`db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND run_mode = ?`,
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
			common.StringUPPER(detailS.TableNameS),
			detailS.RunMode).
		Count(&totals).Error; err != nil {
		return totals, fmt.Errorf("get table [%s] counts failed: %v", table, err)
	}
	return totals, nil
}
