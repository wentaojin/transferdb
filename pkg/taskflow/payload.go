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
package taskflow

import "C"
import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/WentaoJin/transferdb/db"
	"github.com/WentaoJin/transferdb/pkg/config"
	"github.com/WentaoJin/transferdb/zlog"

	"go.uber.org/zap"
)

/*
	全量同步任务
*/
// 全量数据导出导入
func LoaderOracleTableFullDataToMySQL(cfg *config.CfgFile, engine *db.Engine) error {
	startTime := time.Now()
	zlog.Logger.Info("Welcome to transferdb", zap.String("config", cfg.String()))
	zlog.Logger.Info("all full table data loader start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	transferTableSlice, err := getTransferTableSliceByCfg(cfg, engine)
	if err != nil {
		return err
	}

	// 关于全量断点恢复
	//  - 若想断点恢复，设置 enable-checkpoint true,首次一旦运行则 batch 数不能调整，
	//  - 若不想断点恢复或者重新调整 batch 数，设置 enable-checkpoint false,重新运行全量任务
	if !cfg.FullConfig.EnableCheckpoint {
		if err := engine.TruncateMySQLTableFullAndIncrementMetaRecord(cfg.TargetConfig.MetaSchema); err != nil {
			return err
		}
	}
	fullTblSlice, incrementTblSlice, err := engine.IsNotExistFullStageMySQLTableMetaRecord(cfg.SourceConfig.SchemaName, transferTableSlice)
	if err != nil {
		return err
	}

	//根据配置文件生成同步表元数据
	if err := generateCheckpointMeta(cfg, engine, fullTblSlice, incrementTblSlice); err != nil {
		return err
	}

	// 表同步任务开始
	for _, table := range transferTableSlice {
		oracleRowIDSQL, err := getMySQLTableFullMetaRowIDSQL(cfg, engine, table)
		if err != nil {
			return err
		}
		for _, sql := range oracleRowIDSQL {
			// 抽取 Oracle 数据
			columns, rowsResult, err := extractorTableFullRecord(engine, cfg.SourceConfig.SchemaName, table, sql)
			if err != nil {
				return err
			}

			if len(rowsResult) == 0 {
				return fmt.Errorf("oracle schema [%v] table [%v] data return null rows", cfg.SourceConfig.SchemaName, table)
			}

			// 转换 Oracle 数据 -> MySQL
			mysqlSQLSlice, err := translatorTableFullRecord(
				cfg.TargetConfig.SchemaName,
				table,
				columns,
				rowsResult,
				cfg.FullConfig.WorkerThreads,
				cfg.AppConfig.InsertBatchSize,
				true,
			)
			if err != nil {
				return err
			}

			// 应用 Oracle 数据 -> MySQL
			if err := applierTableFullRecord(cfg.TargetConfig.SchemaName, table, cfg.FullConfig.WorkerThreads, mysqlSQLSlice,
				engine); err != nil {
				return err
			}

			// 表数据同步成功，清理断点记录
			if err := clearMySQLTableFullMetaRowIDSQL(cfg, engine, table, sql); err != nil {
				return err
			}
		}
	}

	endTime := time.Now()
	zlog.Logger.Info("all full table data loader finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

/*
	增量同步任务
*/
type IncrementPayload struct {
	Engine           *db.Engine
	TargetSchemaName string
	TargetTableName  string
	OracleSQL        string
	InsertBatchSize  int
	SafeMode         bool
}

// 任务同步
func (p *IncrementPayload) Do() error {
	return nil
}

// 序列化
func (p *IncrementPayload) Marshal() string {
	b, err := json.Marshal(&p)
	if err != nil {
		zlog.Logger.Error("MarshalTaskToString",
			zap.String("string", string(b)),
			zap.String("error", fmt.Sprintf("json marshal task failed: %v", err)))
	}
	return string(b)
}
