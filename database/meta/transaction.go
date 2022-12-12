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

type Transaction struct {
	*Meta
}

func NewCommonModel(m *Meta) *Transaction {
	return &Transaction{Meta: m}
}

func (rw *Transaction) CreateDataCompareMetaAndUpdateWaitSyncMeta(ctx context.Context, dataDiffMeta *DataCompareMeta, waitSyncMeta *WaitSyncMeta) error {
	txn := rw.DB(ctx).Begin()
	err := txn.Create(dataDiffMeta).Error
	if err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("create mysql schema table [data_diff_meta] reocrd failed: %v", err.Error()))
	}
	err = txn.Model(&WaitSyncMeta{}).
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
	txn.Commit()
	return nil
}

func (rw *Transaction) CreateFullSyncMetaAndUpdateWaitSyncMeta(ctx context.Context, fullSyncMeta *FullSyncMeta, waitSyncMeta *WaitSyncMeta) error {
	txn := rw.DB(ctx).Begin()
	err := txn.Create(fullSyncMeta).Error
	if err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_DB, fmt.Errorf("create mysql schema table [full_sync_meta] reocrd failed: %v", err.Error()))
	}
	err = txn.Model(&WaitSyncMeta{}).
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
	txn.Commit()
	return nil
}

func (rw *Transaction) DeleteIncrSyncMetaAndWaitSyncMeta(ctx context.Context, incrSyncMeta *IncrSyncMeta, waitSyncMeta *WaitSyncMeta) error {
	if err := rw.DB(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("source_schema_name = ? and source_table_name = ?",
			common.StringUPPER(incrSyncMeta.SourceSchemaName),
			common.StringUPPER(incrSyncMeta.SourceTableName)).
			Delete(&IncrSyncMeta{}).Error; err != nil {
			return err
		}

		if err := tx.Where("source_schema_name = ? and source_table_name = ? and sync_mode = ?",
			common.StringUPPER(waitSyncMeta.SourceSchemaName),
			common.StringUPPER(waitSyncMeta.SourceTableName),
			waitSyncMeta.SyncMode).
			Delete(&WaitSyncMeta{}).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (rw *Transaction) UpdateIncrSyncMetaSCNByCurrentRedo(ctx context.Context,
	sourceSchemaName string, lastRedoLogMaxSCN, logFileStartSCN, logFileEndSCN uint64) error {
	var logFileSCN uint64
	if logFileEndSCN >= lastRedoLogMaxSCN {
		logFileSCN = logFileStartSCN
	} else {
		logFileSCN = logFileEndSCN
	}

	var tableIncrMeta []IncrSyncMeta
	if err := rw.DB(ctx).Model(IncrSyncMeta{}).Where(
		"source_schema_name = ?",
		common.StringUPPER(sourceSchemaName)).Find(&tableIncrMeta).Error; err != nil {
		return err
	}

	for _, table := range tableIncrMeta {
		if table.GlobalSCN < logFileSCN {
			if err := rw.DB(ctx).Model(&IncrSyncMeta{}).Where(
				"source_schema_name = ? and source_table_name = ?",
				common.StringUPPER(sourceSchemaName),
				common.StringUPPER(table.SourceTableName)).
				Updates(IncrSyncMeta{
					GlobalSCN: logFileSCN,
				}).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

func (rw *Transaction) UpdateIncrSyncMetaSCNByNonCurrentRedo(ctx context.Context,
	sourceSchemaName string, lastRedoLogMaxSCN, logFileStartSCN, logFileEndSCN uint64, transferTableSlice []string) error {
	var logFileSCN uint64
	if logFileEndSCN >= lastRedoLogMaxSCN {
		logFileSCN = logFileStartSCN
	} else {
		logFileSCN = logFileEndSCN
	}

	for _, table := range transferTableSlice {
		if err := rw.DB(ctx).Model(&IncrSyncMeta{}).Where(
			"source_schema_name = ? and source_table_name = ?",
			common.StringUPPER(sourceSchemaName),
			common.StringUPPER(table)).
			Updates(IncrSyncMeta{
				GlobalSCN: logFileSCN,
			}).Error; err != nil {
			return err
		}
	}
	return nil
}

func (rw *Transaction) UpdateIncrSyncMetaSCNByArchivedLog(ctx context.Context,
	sourceSchemaName string, logFileEndSCN uint64, transferTableSlice []string) error {
	for _, table := range transferTableSlice {
		if err := rw.DB(ctx).Model(&IncrSyncMeta{}).Where(
			"source_schema_name = ? and source_table_name = ?",
			common.StringUPPER(sourceSchemaName),
			common.StringUPPER(table)).
			Updates(IncrSyncMeta{
				GlobalSCN:      logFileEndSCN,
				SourceTableSCN: logFileEndSCN,
			}).Error; err != nil {
			return err
		}
	}
	return nil
}
