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
	ID               uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SourceSchemaName string `gorm:"not null;index:idx_schema_table_range;comment:'源端 schema'" json:"source_schema_name"`
	SourceTableName  string `gorm:"not null;index:idx_schema_table_range;comment:'源端表名'" json:"source_table_name"`
	SourceColumnInfo string `gorm:"type:text;comment:'源端查询字段信息'" json:"source_column_info"`
	TargetSchemaName string `gorm:"not null;comment:'目标端 schema'" json:"target_schema_name"`
	TargetTableName  string `gorm:"not null;comment:'目标端表名'" json:"target_table_name"`
	TargetColumnInfo string `gorm:"type:text;comment:'目标端查询字段信息'" json:"target_column_info"`
	WhereColumn      string `gorm:"comment:'查询类型字段列'" json:"where_column"` // 值为空则是指定 WHERE 对比或者 WHERE 1=1 对比
	WhereRange       string `gorm:"not null;index:idx_schema_table_range;comment:'查询 where 条件'" json:"where_range"`
	IsPartition      string `gorm:"comment:'是否是分区表'" json:"is_partition"` // 同步转换统一转换成非分区表，此处只做标志
	*BaseModel
}

func NewDataCompareMetaModel(m *Meta) *DataCompareMeta {
	return &DataCompareMeta{
		BaseModel: &BaseModel{
			Meta: m,
		},
	}
}

func (rw *DataCompareMeta) Create(ctx context.Context, createS interface{}) error {
	if err := rw.DB(ctx).Create(createS.(DataCompareMeta)).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *DataCompareMeta) Detail(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*DataCompareMeta)
	var dsMetas []DataCompareMeta
	if err := rw.DB(ctx).Where("source_schema_name = ? AND source_table_name = ? AND target_schema_name = ? AND target_table_name = ?",
		common.StringUPPER(ds.SourceSchemaName), common.StringUPPER(ds.SourceTableName),
		common.StringUPPER(ds.TargetSchemaName), common.StringUPPER(ds.TargetTableName)).Find(&dsMetas).Error; err != nil {
		return dsMetas, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}

	return dsMetas, nil
}

func (rw *DataCompareMeta) BatchCreate(ctx context.Context, createS interface{}, batchSize int) error {
	if err := rw.DB(ctx).CreateInBatches(createS.([]DataCompareMeta), batchSize).Error; err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, err)
	}
	return nil
}

func (rw *DataCompareMeta) DistinctTableName(ctx context.Context, detailS interface{}) (interface{}, error) {
	ds := detailS.(*DataCompareMeta)
	var tableNames []string
	if err := rw.DB(ctx).Model(&DataCompareMeta{}).
		Where("source_schema_name = ?", common.StringUPPER(ds.SourceSchemaName)).
		Distinct().
		Pluck("source_table_name", &tableNames).Error; err != nil {
		return tableNames, errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("meta schema table [data_diff_meta] query distinct source_table_name record failed: %v", err))
	}
	return tableNames, nil
}

func (rw *DataCompareMeta) Truncate(ctx context.Context, truncateS interface{}) error {
	tableStruct := truncateS.(*DataCompareMeta)
	stmt := &gorm.Statement{DB: rw.GormDB}
	err := stmt.Parse(tableStruct)
	if err != nil {
		return err
	}
	err = rw.DB(ctx).Raw(fmt.Sprintf("TRUNCATE TABLE %s", stmt.Schema.Table)).Error
	if err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("truncate mysql meta schema table [data_diff_meta] reocrd failed: %v", err.Error()))
	}
	return nil
}

func (rw *DataCompareMeta) Delete(ctx context.Context, deleteS interface{}) error {
	ds := deleteS.(*DataCompareMeta)
	if err := rw.DB(ctx).Model(DataCompareMeta{}).
		Where("source_schema_name = ? AND source_table_name = ? AND where_range = ?",
			common.StringUPPER(ds.SourceSchemaName),
			common.StringUPPER(ds.SourceTableName), ds.WhereRange).
		Delete(&DataCompareMeta{}).Error; err != nil {
		return fmt.Errorf("delete meta table [data_compae_meta] record failed: %v", err)
	}
	return nil
}
