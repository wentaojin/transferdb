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
package csv

import (
	"fmt"
	"time"

	"github.com/wentaojin/transferdb/pkg/taskflow"

	"github.com/wentaojin/transferdb/service"
	"go.uber.org/zap"
)

func FullCSVOracleTableRecordToMySQL(cfg *service.CfgFile, engine *service.Engine) error {
	startTime := time.Now()
	service.Logger.Info("all full table data csv start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	// 获取配置文件待同步表列表
	transferTableSlice, err := taskflow.GetTransferTableSliceByCfg(cfg, engine)
	if err != nil {
		return err
	}

	// 判断并记录待同步表列表
	for _, tableName := range transferTableSlice {
		isExist, err := engine.IsExistWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, tableName, taskflow.FullSyncMode)
		if err != nil {
			return err
		}
		if !isExist {
			if err := engine.InitWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, []string{tableName}, taskflow.FullSyncMode); err != nil {
				return err
			}
		}
	}

	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,清理元数据表 [wait_sync_meta],重新运行全量任务
	if !cfg.CSVConfig.EnableCheckpoint {
		if err := engine.TruncateFullSyncTableMetaRecord(cfg.TargetConfig.MetaSchema); err != nil {
			return err
		}
		for _, tableName := range transferTableSlice {
			if err := engine.DeleteWaitSyncTableMetaRecord(cfg.TargetConfig.MetaSchema, cfg.SourceConfig.SchemaName, tableName, taskflow.FullSyncMode); err != nil {
				return err
			}
			// 判断并记录待同步表列表
			isExist, err := engine.IsExistWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, tableName, taskflow.FullSyncMode)
			if err != nil {
				return err
			}
			if !isExist {
				if err := engine.InitWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, []string{tableName}, taskflow.FullSyncMode); err != nil {
					return err
				}
			}
		}
	}

	// 获取等待同步以及未同步完成的表列表
	waitSyncTableMetas, waitSyncTableInfo, err := engine.GetWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, taskflow.FullSyncMode)
	if err != nil {
		return err
	}

	partSyncTableMetas, partSyncTableInfo, err := engine.GetPartSyncTableMetaRecord(cfg.SourceConfig.SchemaName, taskflow.FullSyncMode)
	if err != nil {
		return err
	}
	if len(waitSyncTableMetas) == 0 && len(partSyncTableMetas) == 0 {
		endTime := time.Now()
		service.Logger.Info("all full table data csv finished",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()))
		return nil
	}

	// 判断能否断点续传
	panicCheckpointTables, err := engine.JudgingCheckpointResume(cfg.SourceConfig.SchemaName, partSyncTableMetas)
	if err != nil {
		return err
	}
	if len(panicCheckpointTables) != 0 {
		endTime := time.Now()
		service.Logger.Error("all full table data loader error",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Strings("panic tables", panicCheckpointTables))

		return fmt.Errorf("checkpoint isn't consistent, please reruning [enable-checkpoint = fase]")
	}

	// 启动全量 CSV 任务
	if err = startOracleTableFullCSV(cfg, engine, waitSyncTableInfo, partSyncTableInfo, taskflow.FullSyncMode); err != nil {
		return err
	}

	endTime := time.Now()
	service.Logger.Info("all full table data csv finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
