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

func NewIncrSyncMetaModel(m *Meta) *IncrSyncMeta {
	return &IncrSyncMeta{BaseModel: &BaseModel{
		Meta: m}}
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
