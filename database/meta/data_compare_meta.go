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
	"github.com/wentaojin/transferdb/errors"
	"gorm.io/gorm"
)

// 数据校验元数据表
type DataCompareMeta struct {
	ID          uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS     string `gorm:"type:varchar(15);index:idx_dbtype_st_obj,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT     string `gorm:"type:varchar(15);index:idx_dbtype_st_obj,unique;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS string `gorm:"not null;index:idx_dbtype_st_obj,unique;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS  string `gorm:"not null;index:idx_dbtype_st_obj,unique;comment:'源端表名'" json:"table_name_s"`
	ColumnInfoS string `gorm:"type:text;comment:'源端查询字段信息'" json:"column_info_s"`
	SchemaNameT string `gorm:"not null;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameT  string `gorm:"not null;comment:'目标端表名'" json:"table_name_t"`
	ColumnInfoT string `gorm:"type:text;comment:'目标端查询字段信息'" json:"column_info_t"`
	WhereColumn string `gorm:"comment:'查询类型字段列'" json:"where_column"`
	WhereRange  string `gorm:"not null;index:idx_dbtype_st_obj,unique;comment:'查询 where 条件'" json:"where_range"`
	IsPartition string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
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

func (rw *DataCompareMeta) DetailDataCompareMeta(ctx context.Context, detailS *DataCompareMeta) (interface{}, error) {
	var dsMetas []DataCompareMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND schema_name_t = ? AND table_name_t = ?",
		common.StringUPPER(detailS.DBTypeS), common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS), common.StringUPPER(detailS.TableNameS),
		common.StringUPPER(detailS.SchemaNameT), common.StringUPPER(detailS.TableNameT)).Find(&dsMetas).Error; err != nil {
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

func (rw *DataCompareMeta) DistinctDataCompareMetaTableNameS(ctx context.Context, detailS *DataCompareMeta) ([]string, error) {
	var tableNames []string
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return tableNames, err
	}
	if err := rw.DB(ctx).Model(&DataCompareMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ?",
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS)).
		Distinct().
		Pluck("table_name_s", &tableNames).Error; err != nil {
		return tableNames, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("distinct table [%s] column [table_name_s] failed: %v", table, err))
	}
	return tableNames, nil
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

func (rw *DataCompareMeta) DeleteDataCompareMeta(ctx context.Context, deleteS *DataCompareMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Model(DataCompareMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND where_range = ?",
			common.StringUPPER(deleteS.DBTypeS),
			common.StringUPPER(deleteS.DBTypeT),
			common.StringUPPER(deleteS.SchemaNameS),
			common.StringUPPER(deleteS.TableNameS),
			deleteS.WhereRange).
		Delete(&DataCompareMeta{}).Error; err != nil {
		return fmt.Errorf("delete table [%s] record failed: %v", table, err)
	}
	return nil
}
