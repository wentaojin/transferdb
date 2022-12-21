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
	ID             uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	DBTypeS        string `gorm:"type:varchar(15);index:idx_dbtype_st_map,unique;comment:'源数据库类型'" json:"db_type_s"`
	DBTypeT        string `gorm:"type:varchar(15);index:idx_dbtype_st_map,unique;comment:'目标数据库类型'" json:"db_type_t"`
	SchemaNameS    string `gorm:"not null;index:idx_dbtype_st_map,unique;comment:'源端 schema'" json:"schema_name_s"`
	TableNameS     string `gorm:"not null;index:idx_dbtype_st_map,unique;comment:'源端表名'" json:"table_name_s"`
	Mode           string `gorm:"not null;index:idx_dbtype_st_map,unique;comment:'同步模式'" json:"mode"`
	FullGlobalSCN  uint64 `gorm:"comment:'全量任务 full_sync_meta 全局 SCN'" json:"full_global_scn"`
	FullSplitTimes int    `gorm:"comment:'全量任务 full_sync_meta 切分 SQL 次数'" json:"full_split_times"`
	IsPartition    string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
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

// Detail returns wait sync meta records
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

// Query returns part sync meta records
func (rw *WaitSyncMeta) BatchQueryWaitSyncMeta(ctx context.Context, queryS *WaitSyncMeta) ([]WaitSyncMeta, error) {
	var dsMetas []WaitSyncMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err := rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND full_global_scn > 0 AND full_split_times > 0 AND mode = ?",
		common.StringUPPER(queryS.DBTypeS),
		common.StringUPPER(queryS.DBTypeT),
		common.StringUPPER(queryS.SchemaNameS),
		queryS.Mode).Find(&dsMetas).Error; err != nil {
		return dsMetas, fmt.Errorf("batch query table [%s] record failed: %v", table, err)
	}
	return dsMetas, nil
}

func (rw *WaitSyncMeta) DeleteWaitSyncMeta(ctx context.Context, deleteS *WaitSyncMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND mode = ?",
		common.StringUPPER(deleteS.DBTypeS),
		common.StringUPPER(deleteS.DBTypeT),
		common.StringUPPER(deleteS.SchemaNameS),
		common.StringUPPER(deleteS.TableNameS),
		deleteS.Mode).Delete(&WaitSyncMeta{}).Error
	if err != nil {
		return fmt.Errorf("delete table [%s] reocrd failed: %v", table, err)
	}
	return nil
}

func (rw *WaitSyncMeta) UpdateWaitSyncMeta(ctx context.Context, detailS *WaitSyncMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Model(&WaitSyncMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND mode = ?",
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
			common.StringUPPER(detailS.TableNameS),
			detailS.Mode).
		Updates(map[string]interface{}{
			"FullGlobalSCN":  detailS.FullGlobalSCN,
			"FullSplitTimes": detailS.FullSplitTimes,
			"IsPartition":    detailS.IsPartition,
		}).Error
	if err != nil {
		return fmt.Errorf("update table [%s] record failed: %v", table, err)
	}
	return nil
}

func (rw *WaitSyncMeta) ModifyWaitSyncMetaColumnFullSplitTimesZero(ctx context.Context, detailS *WaitSyncMeta) error {
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Model(&WaitSyncMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND mode = ?",
			common.StringUPPER(detailS.DBTypeS),
			common.StringUPPER(detailS.DBTypeT),
			common.StringUPPER(detailS.SchemaNameS),
			common.StringUPPER(detailS.TableNameS),
			detailS.Mode).
		Updates(map[string]interface{}{
			"FullSplitTimes": detailS.FullSplitTimes,
		}).Error
	if err != nil {
		return fmt.Errorf("modify table [%s] column [full_split_times] failed: %v", table, err)
	}
	return nil
}

func (rw *WaitSyncMeta) DetailWaitSyncMetaBySchemaTableSCN(ctx context.Context, detailS *WaitSyncMeta) ([]WaitSyncMeta, error) {
	var dsMetas []WaitSyncMeta
	table, err := rw.ParseSchemaTable()
	if err != nil {
		return dsMetas, err
	}
	if err = rw.DB(ctx).Where(`db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND full_global_scn > 0 AND full_split_times = 0 and mode = ?`,
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS),
		common.StringUPPER(detailS.TableNameS),
		detailS.Mode).Find(&dsMetas).Error; err != nil {
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
	if err = rw.DB(ctx).Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND full_global_scn > 0 AND full_split_times = 0 AND mode = ?",
		common.StringUPPER(detailS.DBTypeS),
		common.StringUPPER(detailS.DBTypeT),
		common.StringUPPER(detailS.SchemaNameS), detailS.Mode).Find(&tableMetas).Error; err != nil {
		return tableMetas, fmt.Errorf("detail table [%s] record by schema_scn failed: %v", table, err)
	}
	return tableMetas, nil
}
