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

// 增量同步元数据表
type IncrSyncMeta struct {
	ID          uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS     string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT     string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS  string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'源端表名'" json:"table_name_s"`
	SchemaNameT string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'目标 schema'" json:"schema_name_t"`
	TableNameT  string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'目标表名'" json:"table_name_t"`
	GlobalScnS  uint64 `gorm:"comment:'源端全局 SCN'" json:"global_scn_s"`
	TableScnS   uint64 `gorm:"comment:'源端表同步 SCN'" json:"table_scn_s"`
	IsPartition string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	*BaseModel
}

func NewIncrSyncMetaModel(m *Meta) *IncrSyncMeta {
	return &IncrSyncMeta{BaseModel: &BaseModel{
		Meta: m}}
}

func (rw *IncrSyncMeta) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [IncrSyncMeta] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *IncrSyncMeta) CountsIncrSyncMetaBySchemaTable(ctx context.Context, detailS *IncrSyncMeta) (int64, error) {
	var count int64

	table, err := rw.ParseSchemaTable()
	if err != nil {
		return count, err
	}

	if err = rw.DB(ctx).Model(&IncrSyncMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? and table_name_s = ?",
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
			common.StringUPPER(detailS.TableNameS),
		).
		Count(&count).Error; err != nil {
		return count, fmt.Errorf("query table [%s] counts by column [schema and table] failed: %v", table, err)
	}

	return count, nil
}

func (rw *IncrSyncMeta) GetIncrSyncMetaMinGlobalScnSBySchema(ctx context.Context, detailS *IncrSyncMeta) (uint64, error) {
	var globalSCN uint64
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return globalSCN, err
	}
	if err = rw.DB(ctx).Model(&IncrSyncMeta{}).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS),
	).
		Distinct().
		Order("global_scn_s ASC").Limit(1).Pluck("global_scn_s", &globalSCN).Error; err != nil {
		return globalSCN, fmt.Errorf("get table [%s] column [global_scn_s] min value failed: %v", table, err)
	}
	return globalSCN, nil
}

func (rw *IncrSyncMeta) GetIncrSyncMetaMinTableScnSBySchema(ctx context.Context, detailS *IncrSyncMeta) (uint64, error) {
	var sourceTableSCN uint64
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return sourceTableSCN, err
	}
	if err = rw.DB(ctx).Model(&IncrSyncMeta{}).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS),
	).
		Distinct().
		Order("table_scn_s ASC").Limit(1).Pluck("table_scn_s", &sourceTableSCN).Error; err != nil {
		return sourceTableSCN, fmt.Errorf("get table [%s] column [table_scn_s] min value failed: %v", table, err)
	}
	return sourceTableSCN, nil
}

func (rw *IncrSyncMeta) DetailIncrSyncMetaBySchema(ctx context.Context, detailS *IncrSyncMeta) ([]IncrSyncMeta, error) {
	var incrMetas []IncrSyncMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return incrMetas, err
	}
	if err = rw.DB(ctx).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ?",
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
		).
		Find(&incrMetas).Error; err != nil {
		return incrMetas, fmt.Errorf("detail table [%s] record by column [schema_name_s] failed: %v", table, err)
	}
	return incrMetas, nil
}

func (rw *IncrSyncMeta) BatchCreateIncrSyncMeta(ctx context.Context, createS []IncrSyncMeta, batchSize int) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).CreateInBatches(createS, batchSize).Error; err != nil {
		return fmt.Errorf("batch create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *IncrSyncMeta) UpdateIncrSyncMeta(ctx context.Context, detailS *IncrSyncMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Model(&IncrSyncMeta{}).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? and table_name_s = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS),
		common.StringUPPER(detailS.TableNameS)).
		Updates(IncrSyncMeta{GlobalScnS: detailS.GlobalScnS, TableScnS: detailS.TableScnS}).Error; err != nil {
		return fmt.Errorf("update table [%s] record failed: %v", table, err)
	}
	return nil
}
