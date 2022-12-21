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
	"encoding/json"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// 全量同步元数据表
type FullSyncMeta struct {
	ID          uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS     string `gorm:"type:varchar(15);index:idx_dbtype_st_map,unique;index:idx_schema_mode;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT     string `gorm:"type:varchar(15);index:idx_dbtype_st_map,unique;index:idx_schema_mode;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS string `gorm:"type:varchar(15);not null;index:idx_dbtype_st_map,unique;index:idx_schema_mode;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS  string `gorm:"type:varchar(30);not null;index:idx_dbtype_st_map,unique;comment:'源端表名'" json:"table_name_s"`
	SchemaNameT string `gorm:"type:varchar(15);not null;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameT  string `gorm:"type:varchar(30);not null;comment:'目标端表名'" json:"table_name_t"`
	GlobalScnS  uint64 `gorm:"comment:'源端全局 SCN'" json:"global_scn_s"`
	ColumnInfoS string `gorm:"type:text;comment:'源端查询字段信息'" json:"column_info_s"`
	RowidInfoS  string `gorm:"type:varchar(300);not null;index:idx_dbtype_st_map,unique;comment:'表 rowid 切分信息'" json:"rowid_info_s"`
	Mode        string `gorm:"not null;index:idx_dbtype_st_map,unique;index:idx_schema_mode;comment:'同步模式'" json:"mode"`
	IsPartition string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	CSVFile     string `gorm:"type:varchar(300);comment:'csv 文件名'" json:"csv_file"`
	*BaseModel
}

func NewFullSyncMetaModel(m *Meta) *FullSyncMeta {
	return &FullSyncMeta{
		BaseModel: &BaseModel{
			Meta: m},
	}
}

func (rw *FullSyncMeta) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [FullSyncMeta] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *FullSyncMeta) DeleteFullSyncMetaBySchemaSyncMode(ctx context.Context, deleteS *FullSyncMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND mode = ?",
		common.StringUPPER(deleteS.DBTypeS),
		common.StringUPPER(deleteS.DBTypeT),
		common.StringUPPER(deleteS.SchemaNameS),
		deleteS.Mode).Delete(&WaitSyncMeta{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] reocrd failed: %v", table, err)
	}
	return nil
}

func (rw *FullSyncMeta) DistinctFullSyncMetaByTableNameS(ctx context.Context, detailS *FullSyncMeta) ([]string, error) {
	var tableNames []string
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return tableNames, err
	}
	if err = rw.DB(ctx).Model(&FullSyncMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? ",
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS)).
		Distinct().
		Pluck("table_name_s", &tableNames).Error; err != nil {
		return tableNames, fmt.Errorf("distinct table [%s] column [table_name_s] failed: %v", table, err)
	}
	return tableNames, nil
}

func (rw *FullSyncMeta) DetailFullSyncMeta(ctx context.Context, detailS *FullSyncMeta) ([]FullSyncMeta, error) {
	var dsMetas []FullSyncMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err := rw.DB(ctx).Where(detailS).Find(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("detail table [%s] record failed: %v", table, err)
	}
	return dsMetas, nil
}

// 清理并更新同步任务元数据表
// 1、全量每成功同步一张表记录，再清理记录
// 2、更新同步数据表元信息
func (rw *FullSyncMeta) DeleteFullSyncMetaBySchemaTableRowid(ctx context.Context, deleteS *FullSyncMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND mode = ?  AND UPPER(rowid_info_s) = ?",
		common.StringUPPER(deleteS.DBTypeS),
		common.StringUPPER(deleteS.DBTypeT),
		common.StringUPPER(deleteS.SchemaNameS),
		common.StringUPPER(deleteS.TableNameS),
		deleteS.Mode,
		common.StringUPPER(deleteS.RowidInfoS)).Delete(&FullSyncMeta{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] reocrd failed: %v", table, err)
	}
	zap.L().Info("delete table record",
		zap.String("table", deleteS.String()),
		zap.String("status", "success"))
	return nil
}

func (rw *FullSyncMeta) BatchCreateFullSyncMeta(ctx context.Context, createS []FullSyncMeta, batchSize int) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err := rw.DB(ctx).CreateInBatches(createS, batchSize).Error; err != nil {
		return fmt.Errorf("batch create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *FullSyncMeta) String() string {
	jsonStr, _ := json.Marshal(rw)
	return string(jsonStr)
}
