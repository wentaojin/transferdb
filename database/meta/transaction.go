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
		return fmt.Errorf("create table [data_diff_meta] reocrd by transaction failed: %v", err)
	}
	err = txn.Model(&WaitSyncMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND mode = ?",
			common.StringUPPER(waitSyncMeta.DBTypeS),
			common.StringUPPER(waitSyncMeta.DBTypeT),
			common.StringUPPER(waitSyncMeta.SchemaNameS),
			common.StringUPPER(waitSyncMeta.TableNameS),
			waitSyncMeta.Mode).
		Updates(map[string]interface{}{
			"FullGlobalSCN":  waitSyncMeta.FullGlobalSCN,
			"FullSplitTimes": waitSyncMeta.FullSplitTimes,
			"IsPartition":    waitSyncMeta.IsPartition,
		}).Error
	if err != nil {
		return fmt.Errorf("update table [wait_sync_meta] reocrd by transaction failed: %v", err)
	}
	txn.Commit()
	return nil
}

func (rw *Transaction) CreateFullSyncMetaAndUpdateWaitSyncMeta(ctx context.Context, fullSyncMeta *FullSyncMeta, waitSyncMeta *WaitSyncMeta) error {
	txn := rw.DB(ctx).Begin()
	err := txn.Create(fullSyncMeta).Error
	if err != nil {
		return fmt.Errorf("create table [full_sync_meta] reocrd by transaction failed: %v", err)
	}
	err = txn.Model(&WaitSyncMeta{}).
		Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? AND mode = ?",
			common.StringUPPER(waitSyncMeta.DBTypeS),
			common.StringUPPER(waitSyncMeta.DBTypeT),
			common.StringUPPER(waitSyncMeta.SchemaNameS),
			common.StringUPPER(waitSyncMeta.TableNameS),
			waitSyncMeta.Mode).
		Updates(map[string]interface{}{
			"FullGlobalSCN":  waitSyncMeta.FullGlobalSCN,
			"FullSplitTimes": waitSyncMeta.FullSplitTimes,
			"IsPartition":    waitSyncMeta.IsPartition,
		}).Error
	if err != nil {
		return fmt.Errorf("update table [wait_sync_meta] reocrd by transaction failed: %v", err)
	}
	txn.Commit()
	return nil
}

func (rw *Transaction) DeleteIncrSyncMetaAndWaitSyncMeta(ctx context.Context, incrSyncMeta *IncrSyncMeta, waitSyncMeta *WaitSyncMeta) error {
	if err := rw.DB(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ?",
			common.StringUPPER(incrSyncMeta.DBTypeS),
			common.StringUPPER(incrSyncMeta.DBTypeT),
			common.StringUPPER(incrSyncMeta.SchemaNameS),
			common.StringUPPER(incrSyncMeta.TableNameS),
		).
			Delete(&IncrSyncMeta{}).Error; err != nil {
			return fmt.Errorf("delete table [incr_sync_meta] record by transaction failed: %v", err)
		}

		if err := tx.Where("db_type_s = ? AND db_type_t = ? AND schema_name_s = ? AND table_name_s = ? and mode = ?",
			common.StringUPPER(waitSyncMeta.DBTypeS),
			common.StringUPPER(waitSyncMeta.DBTypeT),
			common.StringUPPER(waitSyncMeta.SchemaNameS),
			common.StringUPPER(waitSyncMeta.TableNameS),
			waitSyncMeta.Mode).
			Delete(&WaitSyncMeta{}).Error; err != nil {
			return fmt.Errorf("delete table [wait_sync_meta] record by transaction failed: %v", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (rw *Transaction) UpdateIncrSyncMetaSCNByCurrentRedo(ctx context.Context,
	dbTypeS, dbTypeT, sourceSchemaName string, lastRedoLogMaxSCN, logFileStartSCN, logFileEndSCN uint64) error {
	var logFileSCN uint64
	if logFileEndSCN >= lastRedoLogMaxSCN {
		logFileSCN = logFileStartSCN
	} else {
		logFileSCN = logFileEndSCN
	}

	var tableIncrMeta []IncrSyncMeta
	if err := rw.DB(ctx).Model(IncrSyncMeta{}).Where(
		"db_type_s = ? AND db_type_t = ? AND schema_name_s = ?",
		common.StringUPPER(dbTypeS),
		common.StringUPPER(dbTypeT),
		common.StringUPPER(sourceSchemaName),
	).Find(&tableIncrMeta).Error; err != nil {
		return fmt.Errorf("find table [incr_sync_meta] record by current_redo failed: %v", err)
	}

	for _, table := range tableIncrMeta {
		if table.GlobalScnS < logFileSCN {
			if err := rw.DB(ctx).Model(&IncrSyncMeta{}).Where(
				"db_type_s = ? AND db_type_t = ? AND schema_name_s = ? and table_name_s = ?",
				common.StringUPPER(dbTypeS),
				common.StringUPPER(dbTypeT),
				common.StringUPPER(sourceSchemaName),
				common.StringUPPER(table.TableNameS)).
				Updates(IncrSyncMeta{
					GlobalScnS: logFileSCN,
				}).Error; err != nil {
				return fmt.Errorf("update table [incr_sync_meta] record by current_redo failed: %v", err)
			}
		}
	}
	return nil
}

func (rw *Transaction) UpdateIncrSyncMetaSCNByNonCurrentRedo(ctx context.Context,
	dbTypeS, dbTypeT, sourceSchemaName string, lastRedoLogMaxSCN, logFileStartSCN, logFileEndSCN uint64, transferTableSlice []string) error {
	var logFileSCN uint64
	if logFileEndSCN >= lastRedoLogMaxSCN {
		logFileSCN = logFileStartSCN
	} else {
		logFileSCN = logFileEndSCN
	}

	for _, table := range transferTableSlice {
		if err := rw.DB(ctx).Model(&IncrSyncMeta{}).Where(
			"db_type_s = ? AND db_type_t = ? AND schema_name_s = ? and table_name_s = ?",
			common.StringUPPER(dbTypeS),
			common.StringUPPER(dbTypeT),
			common.StringUPPER(sourceSchemaName),
			common.StringUPPER(table)).
			Updates(IncrSyncMeta{
				GlobalScnS: logFileSCN,
			}).Error; err != nil {
			return fmt.Errorf("update table [incr_sync_meta] record by noncurrent_redo failed: %v", err)
		}
	}
	return nil
}

func (rw *Transaction) UpdateIncrSyncMetaSCNByArchivedLog(ctx context.Context,
	dbTypeS, dbTypeT, sourceSchemaName string, logFileEndSCN uint64, transferTableSlice []string) error {
	for _, table := range transferTableSlice {
		if err := rw.DB(ctx).Model(&IncrSyncMeta{}).Where(
			"db_type_s = ? AND db_type_t = ? AND schema_name_s = ? and table_name_s = ?",
			common.StringUPPER(dbTypeS),
			common.StringUPPER(dbTypeT),
			common.StringUPPER(sourceSchemaName),
			common.StringUPPER(table)).
			Updates(IncrSyncMeta{
				GlobalScnS: logFileEndSCN,
				TableScnS:  logFileEndSCN,
			}).Error; err != nil {
			return fmt.Errorf("update table [incr_sync_meta] record by archivelog failed: %v", err)
		}
	}
	return nil
}
