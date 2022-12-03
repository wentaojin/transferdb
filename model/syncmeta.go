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
package model

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
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

// 全量同步元数据表
type FullSyncMeta struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:idx_schema_table_rowid;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:idx_schema_table_rowid;comment:'源端表名'" json:"source_table_name"`
	TargetSchemaName string `gorm:"not null;comment:'目标端 schema'" json:"target_schema_name"`
	TargetTableName  string `gorm:"not null;comment:'目标端表名'" json:"target_table_name"`
	GlobalSCN        uint64 `gorm:"comment:'全局 SCN'" json:"global_scn"`
	SourceColumnInfo string `gorm:"type:text;comment:'源端查询字段信息'" json:"source_column_info"`
	SourceRowidInfo  string `gorm:"type:varchar(300);not null;index:idx_schema_table_rowid;comment:'表 rowid 切分信息'" json:"source_rowid_info"`
	SyncMode         string `gorm:"not null;index:idx_schema_table_mode,unique;comment:'同步模式'" json:"sync_mode"`
	IsPartition      string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	CSVFile          string `gorm:"type:varchar(300);comment:'csv 文件名'" json:"csv_file"`
	*BaseModel
}

// 增量同步元数据表
type IncrSyncMeta struct {
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:unique_schema_table,unique;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:unique_schema_table,unique;comment:'源端表名'" json:"source_table_name"`
	GlobalSCN        uint64 `gorm:"comment:'全局 SCN'" json:"global_scn"`
	SourceTableSCN   uint64 `gorm:"comment:'表同步 SCN'" json:"source_table_scn"`
	IsPartition      string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	*BaseModel
}

type SyncMeta struct {
	*WaitSyncMeta
	*FullSyncMeta
	*IncrSyncMeta
}

func NewSyncMetaModel(gormDB *gorm.DB) *SyncMeta {
	return &SyncMeta{
		WaitSyncMeta: &WaitSyncMeta{BaseModel: &BaseModel{
			MetaDB: WrapGormDB(gormDB)}},
		FullSyncMeta: &FullSyncMeta{BaseModel: &BaseModel{
			MetaDB: WrapGormDB(gormDB)}},
	}
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

func (rw *FullSyncMeta) DeleteBySchemaSyncMode(ctx context.Context, deleteS interface{}) error {
	ds := deleteS.(*FullSyncMeta)
	err := rw.DB(ctx).Where("source_schema_name = ? AND sync_mode = ?",
		common.StringUPPER(ds.SourceSchemaName),
		ds.SyncMode).Delete(&WaitSyncMeta{}).Error
	if err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("delete meta schema table [full_sync_meta] reocrd failed: %v", err))
	}
	return nil
}

func (rw *FullSyncMeta) DistinctTableName(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*FullSyncMeta)
	var tableNames []string
	if err := rw.DB(ctx).Model(&FullSyncMeta{}).
		Where("source_schema_name = ?",
			common.StringUPPER(ds.SourceSchemaName)).
		Distinct().
		Pluck("source_table_name", &tableNames).Error; err != nil {
		return tableNames, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("meta schema table [full_sync_meta] query distinct source_table_name record failed: %v", err))
	}
	return tableNames, nil
}

func (rw *FullSyncMeta) Detail(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*FullSyncMeta)
	var dsMetas []FullSyncMeta
	if err := rw.DB(ctx).Where(ds).Find(&dsMetas).Error; err != nil {
		return dsMetas, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return dsMetas, nil
}

// 清理并更新同步任务元数据表
// 1、全量每成功同步一张表记录，再清理记录
// 2、更新同步数据表元信息
func (rw *FullSyncMeta) DeleteBySchemaTableRowid(ctx context.Context, deleteS interface{}) error {
	ds := deleteS.(*FullSyncMeta)
	err := rw.DB(ctx).Where("source_schema_name = ? AND source_table_name = ? AND sync_mode = ? AND UPPER(source_rowid_info) = ?",
		common.StringUPPER(ds.SourceSchemaName),
		common.StringUPPER(ds.SourceTableName),
		ds.SyncMode,
		common.StringUPPER(ds.SourceRowidInfo)).Delete(&FullSyncMeta{}).Error
	if err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("delete meta schema table [full_sync_meta] reocrd failed: %v", err))
	}
	zap.L().Info("delete mysql [full_sync_meta] meta",
		zap.String("table", ds.String()),
		zap.String("status", "success"))
	return nil
}

func (rw *FullSyncMeta) BatchCreate(ctx context.Context, createS interface{}, batchSize int) error {
	if err := rw.DB(ctx).CreateInBatches(createS.([]FullSyncMeta), batchSize).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *IncrSyncMeta) CountsBySchemaTable(ctx context.Context, detailS interface{}) (int64, error) {
	var count int64
	ds := detailS.(*IncrSyncMeta)
	if err := rw.DB(ctx).Model(&IncrSyncMeta{}).
		Where("source_schema_name = ? and source_table_name = ?",
			common.StringUPPER(ds.SourceSchemaName),
			common.StringUPPER(ds.SourceTableName)).
		Count(&count).Error; err != nil {
		return count, fmt.Errorf("meta schema table [incr_sync_meta] query record count failed: %v", err)
	}

	return count, nil
}

func (rw *IncrSyncMeta) MinGlobalSCNBySchema(ctx context.Context, detailS interface{}) (uint64, error) {
	ds := detailS.(*IncrSyncMeta)
	var globalSCN uint64
	if err := rw.DB(ctx).Model(&IncrSyncMeta{}).Where("source_schema_name = ?",
		common.StringUPPER(ds.SourceSchemaName)).
		Distinct().
		Order("global_scn ASC").Limit(1).Pluck("global_scn", &globalSCN).Error; err != nil {
		return globalSCN, err
	}
	return globalSCN, nil
}

func (rw *IncrSyncMeta) MinSourceTableSCNBySchema(ctx context.Context, detailS interface{}) (uint64, error) {
	ds := detailS.(*IncrSyncMeta)
	var sourceTableSCN uint64
	if err := rw.DB(ctx).Model(&IncrSyncMeta{}).Where("source_schema_name = ?",
		common.StringUPPER(ds.SourceSchemaName)).
		Distinct().
		Order("source_table_scn ASC").Limit(1).Pluck("source_table_scn", &sourceTableSCN).Error; err != nil {
		return sourceTableSCN, err
	}
	return sourceTableSCN, nil
}

func (rw *IncrSyncMeta) DetailBySchema(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*IncrSyncMeta)
	var incrMetas []IncrSyncMeta
	if err := rw.DB(ctx).
		Where("source_schema_name = ?", common.StringUPPER(ds.SourceSchemaName)).
		Find(&incrMetas).Error; err != nil {
		return incrMetas, err
	}
	return incrMetas, nil
}

func (rw *IncrSyncMeta) BatchCreate(ctx context.Context, createS interface{}, batchSize int) error {
	if err := rw.DB(ctx).CreateInBatches(createS.([]IncrSyncMeta), batchSize).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *IncrSyncMeta) Update(ctx context.Context, detailS interface{}) error {
	ds := detailS.(*IncrSyncMeta)
	if err := rw.DB(ctx).Model(&IncrSyncMeta{}).Where("source_schema_name = ? and source_table_name = ?",
		common.StringUPPER(ds.SourceSchemaName),
		common.StringUPPER(ds.SourceTableName)).
		Updates(IncrSyncMeta{GlobalSCN: ds.GlobalSCN, SourceTableSCN: ds.SourceTableSCN}).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *FullSyncMeta) String() string {
	jsonStr, _ := json.Marshal(rw)
	return string(jsonStr)
}