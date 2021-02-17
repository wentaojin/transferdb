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
package server

import (
	"database/sql"
	"fmt"

	"github.com/xxjwxc/gowp/workpool"

	"github.com/WentaoJin/transferdb/pkg/taskflow"

	"github.com/WentaoJin/transferdb/db"
	"github.com/WentaoJin/transferdb/pkg/config"
)

// 数据库引擎初始化
func NewEngineDB(cfg *config.CfgFile) (*db.Engine, error) {
	var (
		engine *db.Engine
		oraDB  *sql.DB
		err    error
	)
	oraDB, err = db.NewOracleDBEngine(cfg.SourceConfig.DSN)
	if err != nil {
		return engine, err
	}
	engine, err = db.NewMySQLEngineGeneralDB(
		cfg.TargetConfig.Username,
		cfg.TargetConfig.Password,
		cfg.TargetConfig.Host,
		cfg.TargetConfig.Port,
		cfg.TargetConfig.MetaSchema)
	if err != nil {
		return engine, err
	}
	engine.OracleDB = oraDB
	return engine, nil
}

// 全量数据导出导入
func LoaderTableFullData(cfg *config.CfgFile, engine *db.Engine) error {
	isExist, err := engine.IsExistMySQLTableMetaRecord()
	if err != nil {
		return err
	}
	transferTableSlice, err := taskflow.GetTransferTableSliceByCfg(cfg, engine)
	if err != nil {
		return err
	}
	if !isExist {
		if err := taskflow.GenerateCheckpointMeta(cfg, engine, transferTableSlice); err != nil {
			return err
		}
	}

	for _, table := range transferTableSlice {
		// 获取 Oracle 查询 SQL
		oraQuerySlice, mysqlDelSQL, err := taskflow.GenerateMySQLTableFullMetaSQL(cfg, engine, table)
		if err != nil {
			return err
		}

		// 初始化 worker pool
		wp := workpool.New(cfg.FullConfig.WorkerThreads)
		// 表同步任务开始
		for _, oracleSQL := range oraQuerySlice {
			wp.DoWait(func() error {
				if err := taskflow.SyncTableFullRecordToMySQL(cfg, engine, oracleSQL, table, db.InsertBatchSize); err != nil {
					return err
				}
				return nil
			})
		}
		if err := wp.Wait(); err != nil {
			fmt.Println(err)
		}
		// 清理元数据表记录
		if err := engine.ClearMySQLTableFullMetaRecord(table, mysqlDelSQL); err != nil {
			return err
		}
	}
	return nil
}
