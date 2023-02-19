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

// 数据校验元数据表
type DataCompareMeta struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS       string `gorm:"type:varchar(15);index:idx_dbtype_st_obj,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT       string `gorm:"type:varchar(15);index:idx_dbtype_st_obj,unique;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS   string `gorm:"not null;index:idx_dbtype_st_obj,unique;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS    string `gorm:"not null;index:idx_dbtype_st_obj,unique;comment:'源端表名'" json:"table_name_s"`
	ColumnDetailS string `gorm:"type:text;comment:'源端查询字段信息'" json:"column_detail_s"`
	SchemaNameT   string `gorm:"not null;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameT    string `gorm:"not null;comment:'目标端表名'" json:"table_name_t"`
	ColumnDetailT string `gorm:"type:text;comment:'目标端查询字段信息'" json:"column_detail_t"`
	WhereColumn   string `gorm:"comment:'查询类型字段列'" json:"where_column"`
	WhereRange    string `gorm:"not null;index:idx_dbtype_st_obj,unique;comment:'查询 where 条件'" json:"where_range"`
	TaskMode      string `gorm:"not null;index:idx_dbtype_st_obj,unique;comment:'任务模式'" json:"task_mode"`
	TaskStatus    string `gorm:"not null;comment:'数据对比状态,only waiting,success,failed'" json:"task_status"`
	IsPartition   string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	InfoDetail    string `gorm:"not null;comment:'信息详情'" json:"info_detail"`
	ErrorDetail   string `gorm:"not null;comment:'错误详情'" json:"error_detail"`
	*BaseModel
}

func NewDataCompareMetaModel(m *Meta) *DataCompareMeta {
	return &DataCompareMeta{
		BaseModel: &BaseModel{
			Meta: m,
		},
	}
}

func (rw *DataCompareMeta) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [DataCompareMeta] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *DataCompareMeta) CreateDataCompareMeta(ctx context.Context, createS *DataCompareMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Create(createS).Error; err != nil {
		return fmt.Errorf("create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *DataCompareMeta) DetailDataCompareMeta(ctx context.Context, detailS *DataCompareMeta) ([]DataCompareMeta, error) {
	var dsMetas []DataCompareMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err = rw.DB(ctx).Where(detailS).Find(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("detail table [%s] record failed: %v", table, err)
	}
	return dsMetas, nil
}

func (rw *DataCompareMeta) BatchCreateDataCompareMeta(ctx context.Context, createS []DataCompareMeta, batchSize int) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err := rw.DB(ctx).CreateInBatches(createS, batchSize).Error; err != nil {
		return fmt.Errorf("batch create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *DataCompareMeta) TruncateDataCompareMeta(ctx context.Context) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Raw(fmt.Sprintf("TRUNCATE TABLE %s", table)).Error
	if err != nil {
		return fmt.Errorf("truncate table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *DataCompareMeta) UpdateDataCompareMeta(ctx context.Context, deleteS *DataCompareMeta, updates map[string]interface{}) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Model(DataCompareMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND task_mode = ? AND where_range = ?",
			common.StringUPPER(deleteS.DBTypeS),
			common.StringUPPER(deleteS.DBTypeT),
			common.StringUPPER(deleteS.SchemaNameS),
			common.StringUPPER(deleteS.TableNameS),
			common.StringUPPER(deleteS.TaskMode),
			deleteS.WhereRange).
		Updates(updates).Error; err != nil {
		return fmt.Errorf("update table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *DataCompareMeta) CountsErrorDataCompareMeta(ctx context.Context, dataErr *DataCompareMeta) (int64, error) {
	var countsErr int64
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return countsErr, err
	}
	if err := rw.DB(ctx).Model(&DataCompareMeta{}).
		Where(`db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND task_mode = ? AND task_status = ?`,
			common.StringUPPER(dataErr.DBTypeS),
			common.StringUPPER(dataErr.DBTypeT),
			common.StringUPPER(dataErr.SchemaNameS),
			common.StringUPPER(dataErr.TableNameS),
			common.StringUPPER(dataErr.TaskMode),
			dataErr.TaskStatus).
		Count(&countsErr).Error; err != nil {
		return countsErr, fmt.Errorf("get table [%s] counts failed: %v", table, err)
	}
	return countsErr, nil
}

func (rw *DataCompareMeta) CountsDataCompareMetaByTaskTable(ctx context.Context, dataErr *DataCompareMeta) (int64, error) {
	var countsErr int64
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return countsErr, err
	}
	if err := rw.DB(ctx).Model(&DataCompareMeta{}).
		Where(`db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND task_mode = ?`,
			common.StringUPPER(dataErr.DBTypeS),
			common.StringUPPER(dataErr.DBTypeT),
			common.StringUPPER(dataErr.SchemaNameS),
			common.StringUPPER(dataErr.TableNameS),
			common.StringUPPER(dataErr.TaskMode)).
		Count(&countsErr).Error; err != nil {
		return countsErr, fmt.Errorf("get table [%s] counts failed: %v", table, err)
	}
	return countsErr, nil
}

func (rw *DataCompareMeta) DistinctDataCompareMetaTableNameSByTaskStatus(ctx context.Context, detailS *DataCompareMeta) ([]string, error) {
	var tableNames []string
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return tableNames, err
	}
	if err := rw.DB(ctx).Model(&DataCompareMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND task_mode = ? AND task_status = ?",
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
			common.StringUPPER(detailS.TaskMode),
			common.StringUPPER(detailS.TaskStatus)).
		Distinct().
		Pluck("table_name_s", &tableNames).Error; err != nil {
		return tableNames, fmt.Errorf("distinct table [%s] column [table_name_s] failed: %v", table, err)
	}
	return tableNames, nil
}
