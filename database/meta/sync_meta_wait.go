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

// 同步元数据表
type WaitSyncMeta struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS          string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT          string `gorm:"type:varchar(30);index:idx_dbtype_st_map,unique;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS      string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS       string `gorm:"type:varchar(100);not null;index:idx_dbtype_st_map,unique;comment:'源端表名'" json:"table_name_s"`
	TaskMode         string `gorm:"type:varchar(30);not null;index:idx_dbtype_st_map,unique;comment:'任务模式'" json:"task_mode"`
	TaskStatus       string `gorm:"type:varchar(30);not null;comment:'任务状态'" json:"task_status"`
	GlobalScnS       uint64 `gorm:"comment:'全量任务 full_sync_meta 全局 SCN'" json:"global_scn_s"`
	ChunkTotalNums   int64  `gorm:"comment:'全量任务 full_sync_meta 任务切分 chunk 数'" json:"chunk_total_nums"`
	ChunkSuccessNums int64  `gorm:"comment:'全量任务 full_sync_meta 执行成功 chunk 数'" json:"chunk_success_nums"`
	ChunkFailedNums  int64  `gorm:"comment:'全量任务 full_sync_meta 执行失败 chunk 数'" json:"chunk_failed_nums"`
	IsPartition      string `gorm:"type:varchar(10);comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	*BaseModel
}

func NewWaitSyncMetaModel(m *Meta) *WaitSyncMeta {
	return &WaitSyncMeta{BaseModel: &BaseModel{
		Meta: m}}
}

func (rw *WaitSyncMeta) ParseSchemaTable() (string, error) {
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(rw)
	if err != nil {
		return "", fmt.Errorf("parse struct [WaitSyncMeta] get table_name failed: %v", err)
	}
	return stmt.Schema.Table, nil
}

func (rw *WaitSyncMeta) CreateWaitSyncMeta(ctx context.Context, createS *WaitSyncMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	if err = rw.DB(ctx).Create(createS).Error; err != nil {
		return fmt.Errorf("create table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *WaitSyncMeta) DetailWaitSyncMeta(ctx context.Context, detailS *WaitSyncMeta) ([]WaitSyncMeta, error) {
	var dsMetas []WaitSyncMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err = rw.DB(ctx).Where(detailS).Find(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("detail table [%s] record failed: %v", table, err)
	}
	return dsMetas, nil
}

func (rw *WaitSyncMeta) DetailWaitSyncMetaSuccessTables(ctx context.Context, detailS *WaitSyncMeta) ([]string, error) {
	var dsMetas []string
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err = rw.DB(ctx).Model(&WaitSyncMeta{}).Select(`table_name_s`).Where(`db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND task_mode = ? AND task_status = ?`,
		detailS.DBTypeS,
		detailS.DBTypeT,
		detailS.SchemaNameS,
		detailS.TaskMode,
		detailS.TaskStatus).Scan(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("detail success table [%s] record failed: %v", table, err)
	}
	return dsMetas, nil
}

func (rw *WaitSyncMeta) QueryWaitSyncMetaByPartTask(ctx context.Context, queryS *WaitSyncMeta) ([]WaitSyncMeta, error) {
	var dsMetas []WaitSyncMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND global_scn_s > 0 AND task_mode = ? AND task_status = ?",
		common.StringUPPER(queryS.DBTypeS),
		common.StringUPPER(queryS.DBTypeT),
		common.StringUPPER(queryS.SchemaNameS),
		queryS.TaskMode,
		queryS.TaskStatus).Find(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("query table [%s] record failed: %v", table, err)
	}
	return dsMetas, nil
}

func (rw *WaitSyncMeta) DeleteWaitSyncMeta(ctx context.Context, deleteS *WaitSyncMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND task_mode = ?",
		common.StringUPPER(deleteS.DBTypeS),
		common.StringUPPER(deleteS.DBTypeT),
		common.StringUPPER(deleteS.SchemaNameS),
		common.StringUPPER(deleteS.TableNameS),
		deleteS.TaskMode).Delete(&WaitSyncMeta{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] reocrd failed: %v", table, err)
	}
	return nil
}

func (rw *WaitSyncMeta) DeleteWaitSyncMetaSuccessTables(ctx context.Context, deleteS *WaitSyncMeta, tables []string) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND task_mode = ? AND task_status = ? AND AND table_name_s IN (?)",
		common.StringUPPER(deleteS.DBTypeS),
		common.StringUPPER(deleteS.DBTypeT),
		common.StringUPPER(deleteS.SchemaNameS),
		deleteS.TaskMode,
		deleteS.TaskStatus,
		tables).Delete(&WaitSyncMeta{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] reocrd failed: %v", table, err)
	}
	return nil
}

func (rw *WaitSyncMeta) UpdateWaitSyncMeta(ctx context.Context, detailS *WaitSyncMeta, updates map[string]interface{}) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Model(&WaitSyncMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND task_mode = ?",
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
			common.StringUPPER(detailS.TableNameS),
			detailS.TaskMode).
		Updates(updates).Error
	if err != nil {
		return fmt.Errorf("update table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *WaitSyncMeta) CountsErrWaitSyncMetaBySchema(ctx context.Context, dataErr *WaitSyncMeta) (int64, error) {
	var countsErr int64
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return countsErr, err
	}
	if err := rw.DB(ctx).Model(&WaitSyncMeta{}).
		Where(`db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND task_mode = ? AND task_status = ?`,
			common.StringUPPER(dataErr.DBTypeS),
			common.StringUPPER(dataErr.DBTypeT),
			common.StringUPPER(dataErr.SchemaNameS),
			common.StringUPPER(dataErr.TaskMode),
			common.StringUPPER(dataErr.TaskStatus)).
		Count(&countsErr).Error; err != nil {
		return countsErr, fmt.Errorf("get table [%s] counts failed: %v", table, err)
	}
	return countsErr, nil
}

func (rw *WaitSyncMeta) DetailWaitSyncMetaBySchemaTableSCN(ctx context.Context, detailS *WaitSyncMeta) ([]WaitSyncMeta, error) {
	var dsMetas []WaitSyncMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err = rw.DB(ctx).Where(`db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND global_scn_s > 0 AND task_mode = ? AND task_status = ?`,
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS),
		common.StringUPPER(detailS.TableNameS),
		detailS.TaskMode,
		detailS.TaskStatus).Find(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("detail table [%s] record by schema_table_scn failed: %v", table, err)
	}

	return dsMetas, nil
}

func (rw *WaitSyncMeta) DetailWaitSyncMetaBySchema(ctx context.Context, detailS *WaitSyncMeta) ([]WaitSyncMeta, error) {
	var tableMetas []WaitSyncMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return tableMetas, err
	}
	if err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND global_scn_s > 0 AND task_mode = ? AND task_status = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS),
		detailS.TaskMode,
		detailS.TaskStatus).Find(&tableMetas).Error; err != nil {
		return tableMetas, fmt.Errorf("detail table [%s] record by schema_scn failed: %v", table, err)
	}
	return tableMetas, nil
}
