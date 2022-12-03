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
package o2m

import (
	"context"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/model"
	"github.com/wentaojin/transferdb/module/query/mysql"
	"github.com/wentaojin/transferdb/module/query/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"time"
)

type Table struct {
	Ctx       context.Context
	SyncMeta  model.FullSyncMeta
	Oracle    *oracle.Oracle
	BatchSize int
}

func NewTable(ctx context.Context, syncMeta model.FullSyncMeta,
	oracle *oracle.Oracle, batchSize int) *Table {
	return &Table{
		Ctx:       ctx,
		SyncMeta:  syncMeta,
		Oracle:    oracle,
		BatchSize: batchSize,
	}
}

func (t *Table) GetTableRows() ([]string, []string, error) {
	startTime := time.Now()
	querySQL := common.StringsBuilder(`SELECT `, t.SyncMeta.SourceColumnInfo, ` FROM `, t.SyncMeta.SourceSchemaName, `.`, t.SyncMeta.SourceTableName, ` WHERE `, t.SyncMeta.SourceRowidInfo)

	columnFields, rowResults, err := t.Oracle.GetOracleTableRowsData(querySQL, t.BatchSize)
	if err != nil {
		return columnFields, rowResults, err
	}

	endTime := time.Now()
	zap.L().Info("source schema table rowid data extractor finished",
		zap.String("schema", t.SyncMeta.SourceSchemaName),
		zap.String("table", t.SyncMeta.SourceTableName),
		zap.String("rowid", t.SyncMeta.SourceRowidInfo),
		zap.String("sql", querySQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return columnFields, rowResults, nil
}

type Chunk struct {
	Ctx           context.Context
	SyncMeta      model.FullSyncMeta
	ApplyThreads  int
	BatchSize     int
	SafeMode      bool
	MySQL         *mysql.MySQL
	Oracle        *oracle.Oracle
	SourceColumns []string
	BatchResults  []string
}

func NewChunk(ctx context.Context, syncMeta model.FullSyncMeta,
	oracle *oracle.Oracle, mysql *mysql.MySQL,
	sourceColumns, batchResults []string, applyThreads, batchSize int, safeMode bool) *Chunk {
	return &Chunk{
		Ctx:           ctx,
		SyncMeta:      syncMeta,
		ApplyThreads:  applyThreads,
		BatchSize:     batchSize,
		SafeMode:      safeMode,
		MySQL:         mysql,
		Oracle:        oracle,
		SourceColumns: sourceColumns,
		BatchResults:  batchResults,
	}
}

func (t *Chunk) TranslateTableRows() error {
	if len(t.BatchResults) == 0 {
		return nil
	}
	return nil
}

func (t *Chunk) ApplyTableRows() error {
	startTime := time.Now()
	zap.L().Info("target schema table rowid data applier start",
		zap.String("schema", t.SyncMeta.TargetSchemaName),
		zap.String("table", t.SyncMeta.TargetTableName),
		zap.String("rowid", t.SyncMeta.SourceRowidInfo))

	if len(t.BatchResults) == 0 {
		zap.L().Warn("oracle schema table rowid data return null rows, skip",
			zap.String("schema", t.SyncMeta.SourceSchemaName),
			zap.String("table", t.SyncMeta.SourceTableName),
			zap.String("info", common.StringsBuilder(`SELECT `, t.SyncMeta.SourceColumnInfo, ` FROM `, t.SyncMeta.SourceSchemaName, `.`, t.SyncMeta.SourceTableName, ` WHERE `, t.SyncMeta.SourceRowidInfo)))

		// 清理 full_sync_meta 记录
		err := model.NewSyncMetaModel(t.MySQL.GormDB).FullSyncMeta.DeleteBySchemaTableRowid(t.Ctx, &model.FullSyncMeta{
			SourceSchemaName: t.SyncMeta.SourceSchemaName,
			SourceTableName:  t.SyncMeta.SourceTableName,
			SourceRowidInfo:  t.SyncMeta.SourceRowidInfo,
			SyncMode:         common.FullO2MMode,
		})
		if err != nil {
			return err
		}
		return nil
	}

	g := &errgroup.Group{}
	g.SetLimit(t.ApplyThreads)
	for _, result := range t.BatchResults {
		valArgs := result
		g.Go(func() error {
			err := t.MySQL.WriteMySQLTable(common.StringsBuilder(GenMySQLInsertSQLStmtPrefix(
				t.SyncMeta.TargetSchemaName,
				t.SyncMeta.SourceTableName,
				t.SourceColumns,
				t.SafeMode), valArgs))
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// 清理 full_sync_meta 记录
	err := model.NewSyncMetaModel(t.MySQL.GormDB).FullSyncMeta.DeleteBySchemaTableRowid(t.Ctx, &model.FullSyncMeta{
		SourceSchemaName: t.SyncMeta.SourceSchemaName,
		SourceTableName:  t.SyncMeta.SourceTableName,
		SourceRowidInfo:  t.SyncMeta.SourceRowidInfo,
		SyncMode:         common.FullO2MMode,
	})
	if err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("target schema table rowid data applier finished",
		zap.String("schema", t.SyncMeta.TargetSchemaName),
		zap.String("table", t.SyncMeta.TargetTableName),
		zap.String("rowid", t.SyncMeta.SourceRowidInfo),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
