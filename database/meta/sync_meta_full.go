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
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS       string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;index:idx_schema_mode;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT       string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;index:idx_schema_mode;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS   string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;index:idx_schema_mode;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS    string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'源端表名'" json:"table_name_s"`
	SchemaNameT   string `gorm:"type:varchar(100);not null;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameT    string `gorm:"type:varchar(100);not null;comment:'目标端表名'" json:"table_name_t"`
	GlobalScnS    uint64 `gorm:"comment:'源端全局 SCN'" json:"global_scn_s"`
	ColumnDetailS string `gorm:"type:text;comment:'源端查询字段信息'" json:"column_detail_s"`
	ChunkDetailS  string `gorm:"type:varchar(300);not null;index:idx_dbtype_st_map,unique;comment:'表 chunk 切分信息'" json:"chunk_detail_s"`
	TaskMode      string `gorm:"type:varchar(30);not null;index:idx_dbtype_st_map,unique;index:idx_schema_mode;comment:'任务模式'" json:"task_mode"`
	TaskStatus    string `gorm:"type:varchar(30);not null;comment:'任务 chunk 状态'" json:"task_status"`
	CSVFile       string `gorm:"type:varchar(300);comment:'csv 文件名'" json:"csv_file"`
	IsPartition   string `gorm:"type:varchar(10);comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	InfoDetail    string `gorm:"type:text;not null;comment:'信息详情'" json:"info_detail"`
	ErrorDetail   string `gorm:"type:text;not null;comment:'错误详情'" json:"error_detail"`
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
	err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND task_mode = ?",
		common.StringUPPER(deleteS.DBTypeS),
		common.StringUPPER(deleteS.DBTypeT),
		common.StringUPPER(deleteS.SchemaNameS),
		deleteS.TaskMode).Delete(&FullSyncMeta{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] reocrd failed: %v", table, err)
	}
	return nil
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

func (rw *FullSyncMeta) DeleteFullSyncMetaBySchemaTableRowid(ctx context.Context, deleteS *FullSyncMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND task_mode = ?  AND UPPER(chunk_detail_s) = ?",
		common.StringUPPER(deleteS.DBTypeS),
		common.StringUPPER(deleteS.DBTypeT),
		common.StringUPPER(deleteS.SchemaNameS),
		common.StringUPPER(deleteS.TableNameS),
		deleteS.TaskMode,
		common.StringUPPER(deleteS.ChunkDetailS)).Delete(&FullSyncMeta{}).Error
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

func (rw *FullSyncMeta) UpdateFullSyncMetaChunk(ctx context.Context, detailS *FullSyncMeta, updates map[string]interface{}) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Model(FullSyncMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND task_mode = ? AND chunk_detail_s = ?",
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
			common.StringUPPER(detailS.TableNameS),
			common.StringUPPER(detailS.TaskMode),
			detailS.ChunkDetailS).
		Updates(updates).Error; err != nil {
		return fmt.Errorf("update table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *FullSyncMeta) CountsErrorFullSyncMeta(ctx context.Context, dataErr *FullSyncMeta) (int64, error) {
	var countsErr int64
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return countsErr, err
	}
	if err := rw.DB(ctx).Model(&FullSyncMeta{}).
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

func (rw *FullSyncMeta) CountsFullSyncMetaByTaskTable(ctx context.Context, dataErr *FullSyncMeta) (int64, error) {
	var countsErr int64
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return countsErr, err
	}
	if err := rw.DB(ctx).Model(&FullSyncMeta{}).
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

func (rw *FullSyncMeta) String() string {
	jsonStr, _ := json.Marshal(rw)
	return string(jsonStr)
}
