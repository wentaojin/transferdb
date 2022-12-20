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
)

// 同步元数据表
type WaitSyncMeta struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:idx_schema_table_mode,unique;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:idx_schema_table_mode,unique;comment:'源端表名'" json:"source_table_name"`
	SyncMode         string `gorm:"not null;index:idx_schema_table_mode,unique;comment:'同步模式'" json:"sync_mode"`
	FullGlobalSCN    uint64 `gorm:"comment:'全量全局 SCN'" json:"full_global_scn"`
	FullSplitTimes   int    `gorm:"comment:'全量任务切分 SQL 次数'" json:"full_split_times"`
	IsPartition      string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	*BaseModel
}

func NewWaitSyncMetaModel(m *Meta) *WaitSyncMeta {
	return &WaitSyncMeta{BaseModel: &BaseModel{
		Meta: m}}
}

func (rw *WaitSyncMeta) Create(ctx context.Context, createS interface{}) error {
	if err := rw.DB(ctx).Create(createS.(*WaitSyncMeta)).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

// Detail returns wait sync meta records
func (rw *WaitSyncMeta) Detail(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*WaitSyncMeta)
	var dsMetas []WaitSyncMeta
	if err := rw.DB(ctx).Where(ds).Find(&dsMetas).Error; err != nil {
		return dsMetas, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}

	return dsMetas, nil
}

// Query returns part sync meta records
func (rw *WaitSyncMeta) Query(ctx context.Context, queryS interface{}) (interface{}, error) {
	ds := queryS.(*WaitSyncMeta)
	var dsMetas []WaitSyncMeta
	if err := rw.DB(ctx).Where("source_schema_name = ? AND full_global_scn > 0 AND full_split_times > 0 and sync_mode = ?", common.StringUPPER(ds.SourceSchemaName), ds.SyncMode).Find(&dsMetas).Error; err != nil {
		return dsMetas, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return dsMetas, nil
}

func (rw *WaitSyncMeta) Delete(ctx context.Context, deleteS interface{}) error {
	ds := deleteS.(*WaitSyncMeta)
	err := rw.DB(ctx).Where("source_schema_name = ? AND source_table_name = ? AND sync_mode = ?",
		common.StringUPPER(ds.SourceSchemaName),
		common.StringUPPER(ds.SourceTableName),
		ds.SyncMode).Delete(&WaitSyncMeta{}).Error
	if err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("delete meta schema table [wait_sync_meta] reocrd failed: %v", err))
	}
	return nil
}

func (rw *WaitSyncMeta) Update(ctx context.Context, detailS interface{}) error {
	waitSyncMeta := detailS.(*WaitSyncMeta)
	err := rw.DB(ctx).Model(&WaitSyncMeta{}).
		Where("source_schema_name = ? AND source_table_name = ? AND sync_mode = ?",
			common.StringUPPER(waitSyncMeta.SourceSchemaName),
			common.StringUPPER(waitSyncMeta.SourceTableName),
			waitSyncMeta.SyncMode).
		Updates(map[string]interface{}{
			"FullGlobalSCN":  waitSyncMeta.FullGlobalSCN,
			"FullSplitTimes": waitSyncMeta.FullSplitTimes,
			"IsPartition":    waitSyncMeta.IsPartition,
		}).Error
	if err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("update mysql schema table [wait_sync_meta] reocrd failed: %v", err.Error()))
	}
	return nil
}

func (rw *WaitSyncMeta) ModifyFullSplitTimesZero(ctx context.Context, detailS interface{}) error {
	waitSyncMeta := detailS.(*WaitSyncMeta)
	err := rw.DB(ctx).Model(&WaitSyncMeta{}).
		Where("source_schema_name = ? AND source_table_name = ? AND sync_mode = ?",
			common.StringUPPER(waitSyncMeta.SourceSchemaName),
			common.StringUPPER(waitSyncMeta.SourceTableName),
			waitSyncMeta.SyncMode).
		Updates(map[string]interface{}{
			"FullSplitTimes": waitSyncMeta.FullSplitTimes,
		}).Error
	if err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("modify mysql schema table [wait_sync_meta] reocrd failed: %v", err.Error()))
	}
	return nil
}

func (rw *WaitSyncMeta) DetailBySchemaTableSCN(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*WaitSyncMeta)
	var dsMetas []WaitSyncMeta
	if err := rw.DB(ctx).Where(`source_schema_name = ? AND source_table_name = ? AND full_global_scn > 0 AND full_split_times = 0 and sync_mode = ?`,
		common.StringUPPER(ds.SourceSchemaName),
		common.StringUPPER(ds.SourceTableName),
		ds.SyncMode).Find(&dsMetas).Error; err != nil {
		return dsMetas, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}

	return dsMetas, nil
}

func (rw *WaitSyncMeta) DetailBySchema(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*WaitSyncMeta)
	var tableMetas []WaitSyncMeta
	if err := rw.DB(ctx).Where("source_schema_name = ? AND full_global_scn > 0 AND full_split_times = 0 AND sync_mode = ?",
		common.StringUPPER(ds.SourceSchemaName), ds.SyncMode).Find(&tableMetas).Error; err != nil {
		return tableMetas, err
	}
	return tableMetas, nil
}
