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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/wentaojin/transferdb/pkg/taskflow"

	"go.uber.org/zap"

	"github.com/wentaojin/transferdb/service"
	"github.com/xxjwxc/gowp/workpool"
)

func startOracleTableFullCSV(cfg *service.CfgFile, engine *service.Engine, waitSyncTableInfo, partSyncTableInfo []string, syncMode string, oracleCollation bool) error {
	service.Logger.Info("all full table data csv list",
		zap.Strings("wait sync tables", waitSyncTableInfo),
		zap.Strings("part sync tables", partSyncTableInfo))

	oracleCharacterSet, err := engine.GetOracleDBCharacterSet()
	if err != nil {
		return err
	}

	// 优先存在断点的表同步
	if len(partSyncTableInfo) > 0 {
		if err = startOracleTableConsumeByCheckpoint(cfg, engine, partSyncTableInfo, oracleCharacterSet, syncMode); err != nil {
			return err
		}
	}
	if len(waitSyncTableInfo) > 0 {
		// 初始化表任务
		if err = initOracleTableConsumeRowID(cfg, engine, waitSyncTableInfo, taskflow.FullSyncMode, oracleCollation); err != nil {
			return err
		}

		if err = startOracleTableConsumeByCheckpoint(cfg, engine, waitSyncTableInfo, oracleCharacterSet, syncMode); err != nil {
			return err
		}
	}
	return nil
}

func startOracleTableConsumeByCheckpoint(cfg *service.CfgFile, engine *service.Engine, syncTableInfo []string, sourceCharset, syncMode string) error {
	wp := workpool.New(cfg.CSVConfig.TableThreads)
	for _, tbl := range syncTableInfo {
		table := tbl
		wp.Do(func() error {
			if err := syncOracleRowsByRowID(cfg, engine, sourceCharset, table, syncMode); err != nil {
				return fmt.Errorf("sync oracle table rows by rowid failed: %v", err)
			}
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return fmt.Errorf("sync oracle table rows by checkpoint failed: %v", err)
	}
	if !wp.IsDone() {
		return fmt.Errorf("sync oracle table rows by checkpoint failed, please rerunning")
	}
	return nil
}

func initOracleTableConsumeRowID(cfg *service.CfgFile, engine *service.Engine, waitSyncTableInfo []string, syncMode string, oraCollation bool) error {
	wp := workpool.New(cfg.CSVConfig.TaskThreads)

	for idx, tbl := range waitSyncTableInfo {
		table := tbl
		workerID := idx
		wp.Do(func() error {
			startTime := time.Now()
			service.Logger.Info("single full table init scn start",
				zap.String("schema", cfg.SourceConfig.SchemaName),
				zap.String("table", table))

			// 全量同步前，获取 SCN 以及初始化元数据表
			globalSCN, err := engine.GetOracleCurrentSnapshotSCN()
			if err != nil {
				return err
			}

			// Date/Timestamp 字段类型格式化
			// Interval Year/Day 数据字符 TO_CHAR 格式化
			sourceColumnInfo, err := engine.AdjustTableSelectColumn(cfg.SourceConfig.SchemaName, table, oraCollation)
			if err != nil {
				return err
			}

			if err = engine.InitWaitAndFullSyncMetaRecord(strings.ToUpper(cfg.SourceConfig.SchemaName),
				table, sourceColumnInfo, strings.ToUpper(cfg.TargetConfig.SchemaName), table, workerID, globalSCN,
				cfg.CSVConfig.Rows, cfg.AppConfig.InsertBatchSize, cfg.CSVConfig.OutputDir, syncMode); err != nil {
				return err
			}

			endTime := time.Now()
			service.Logger.Info("single full table init scn finished",
				zap.String("schema", cfg.SourceConfig.SchemaName),
				zap.String("table", table),
				zap.String("cost", endTime.Sub(startTime).String()))
			return nil
		})
	}
	if err := wp.Wait(); err != nil {
		return err
	}
	if !wp.IsDone() {
		return fmt.Errorf("init oracle table rowid by scn failed, please rerunning")
	}
	return nil
}

func syncOracleRowsByRowID(cfg *service.CfgFile, engine *service.Engine, sourceCharset, sourceTableName, syncMode string) error {
	startTime := time.Now()
	service.Logger.Info("single full table data sync start",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("charset", sourceCharset),
		zap.String("table", sourceTableName))

	fullSyncMetas, err := engine.GetFullSyncMetaRowIDRecord(cfg.SourceConfig.SchemaName, sourceTableName)
	if err != nil {
		return err
	}

	wp := workpool.New(cfg.CSVConfig.SQLThreads)
	for _, m := range fullSyncMetas {
		meta := m
		wp.Do(func() error {
			// 抽取 Oracle 数据
			var (
				columnFields []string
				rowsResult   *sql.Rows
			)
			columnFields, rowsResult, err = extractorTableFullRecord(engine, cfg.SourceConfig.SchemaName, meta.SourceTableName, meta.RowidSQL)
			if err != nil {
				return err
			}

			// 转换/应用 Oracle CSV 数据
			if err = applierTableFullRecord(
				cfg.TargetConfig.SchemaName, meta.SourceTableName, meta.RowidSQL,
				translatorTableFullRecord(
					cfg.TargetConfig.SchemaName, meta.SourceTableName, sourceCharset,
					columnFields, engine, meta.SourceSchemaName, meta.SourceTableName,
					meta.RowidSQL, rowsResult, cfg.CSVConfig, meta.CSVFile)); err != nil {
				return err
			}

			// 清理记录
			if err = engine.ModifyFullSyncTableMetaRecord(
				cfg.TargetConfig.MetaSchema,
				cfg.SourceConfig.SchemaName, meta.SourceTableName, meta.RowidSQL); err != nil {
				return err
			}
			return nil
		})
	}
	if err = wp.Wait(); err != nil {
		return err
	}

	endTime := time.Now()
	if !wp.IsDone() {
		service.Logger.Fatal("single full table data loader failed",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("table", sourceTableName),
			zap.String("cost", endTime.Sub(startTime).String()))
		return fmt.Errorf("oracle schema [%s] single full table [%v] data loader failed",
			cfg.SourceConfig.SchemaName, sourceTableName)
	}

	// 更新记录
	if err = engine.ModifyWaitSyncTableMetaRecord(
		cfg.TargetConfig.MetaSchema,
		cfg.SourceConfig.SchemaName, sourceTableName, syncMode); err != nil {
		return err
	}

	service.Logger.Info("single full table data loader finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("table", sourceTableName),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
