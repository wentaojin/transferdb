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
	"gorm.io/gorm/clause"
)

type ChunkErrorDetail struct {
	ID           uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS      string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT      string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS  string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS   string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'源端表名'" json:"table_name_s"`
	SchemaNameT  string `gorm:"type:varchar(100);not null;comment:'目标端 schema'" json:"schema_name_t"`
	TableNameT   string `gorm:"type:varchar(100);not null;comment:'目标端表名'" json:"table_name_t"`
	TaskMode     string `gorm:"type:varchar(30);not null;index:idx_dbtype_st_map,unique;comment:'任务模式'" json:"task_mode"`
	ChunkDetailS string `gorm:"type:varchar(300);not null;index:idx_dbtype_st_map,unique;comment:'表 chunk 切分信息'" json:"chunk_detail_s"`
	InfoDetail   string `gorm:"type:longtext;not null;comment:'信息详情'" json:"info_detail"`
	ErrorSQL     string `gorm:"type:longtext;not null;index:idx_dbtype_st_map,unique;comment:'错误 SQL'" json:"error_sql"`
	ErrorDetail  string `gorm:"type:longtext;not null;comment:'错误详情'" json:"error_detail"`
	*BaseModel
}

func NewChunkErrorDetailModel(m *Meta) *ChunkErrorDetail {
	return &ChunkErrorDetail{
		BaseModel: &BaseModel{
			Meta: m},
	}
}

func (rw *ChunkErrorDetail) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [ChunkErrorDetail] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *ChunkErrorDetail) DeleteChunkErrorDetailBySchemaTaskMode(ctx context.Context, deleteS *ChunkErrorDetail) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND task_mode = ?",
		common.StringUPPER(deleteS.DBTypeS),
		common.StringUPPER(deleteS.DBTypeT),
		common.StringUPPER(deleteS.SchemaNameS),
		deleteS.TaskMode).Delete(&ChunkErrorDetail{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] reocrd failed: %v", table, err)
	}
	return nil
}

func (rw *ChunkErrorDetail) CreateChunkErrorDetail(ctx context.Context, createS *ChunkErrorDetail) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "db_type_s"},
			{Name: "db_type_t"},
			{Name: "schema_name_s"},
			{Name: "table_name_s"},
			{Name: "task_mode"},
			{Name: "chunk_detail_s"},
			{Name: "error_sql"}},
		DoUpdates: clause.AssignmentColumns([]string{"schema_name_t", "table_name_t", "info_detail", "error_detail"}),
	}).Create(createS).Error; err != nil {
		return fmt.Errorf("create table [%s] record failed: %v", table, err)
	}
	return nil
}
