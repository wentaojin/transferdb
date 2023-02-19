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
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"time"
)

type Table struct {
	Ctx       context.Context
	SyncMeta  meta.FullSyncMeta
	Oracle    *oracle.Oracle
	BatchSize int
}

func NewTable(ctx context.Context, syncMeta meta.FullSyncMeta,
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
	querySQL := common.StringsBuilder(`SELECT `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` WHERE `, t.SyncMeta.ChunkDetailS)

	columnFields, rowResults, err := t.Oracle.GetOracleTableRowsData(querySQL, t.BatchSize)
	if err != nil {
		return columnFields, rowResults, err
	}

	endTime := time.Now()
	zap.L().Info("source schema table rowid data extractor finished",
		zap.String("schema", t.SyncMeta.SchemaNameS),
		zap.String("table", t.SyncMeta.TableNameS),
		zap.String("rowid", t.SyncMeta.ChunkDetailS),
		zap.String("sql", querySQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return columnFields, rowResults, nil
}

type Chunk struct {
	Ctx           context.Context
	SyncMeta      meta.FullSyncMeta
	ApplyThreads  int
	BatchSize     int
	SafeMode      bool
	MySQL         *mysql.MySQL
	Oracle        *oracle.Oracle
	MetaDB        *meta.Meta
	SourceColumns []string
	BatchResults  []string
}

func NewChunk(ctx context.Context, syncMeta meta.FullSyncMeta,
	oracle *oracle.Oracle, mysql *mysql.MySQL, metaDB *meta.Meta,
	sourceColumns, batchResults []string, applyThreads, batchSize int, safeMode bool) *Chunk {
	return &Chunk{
		Ctx:           ctx,
		SyncMeta:      syncMeta,
		ApplyThreads:  applyThreads,
		BatchSize:     batchSize,
		SafeMode:      safeMode,
		MySQL:         mysql,
		Oracle:        oracle,
		MetaDB:        metaDB,
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
		zap.String("schema", t.SyncMeta.SchemaNameT),
		zap.String("table", t.SyncMeta.TableNameT),
		zap.String("rowid", t.SyncMeta.ChunkDetailS))

	if len(t.BatchResults) == 0 {
		zap.L().Warn("oracle schema table rowid data return null rows, skip",
			zap.String("schema", t.SyncMeta.SchemaNameS),
			zap.String("table", t.SyncMeta.TableNameS),
			zap.String("info", common.StringsBuilder(`SELECT `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` WHERE `, t.SyncMeta.ChunkDetailS)))
		return nil
	}

	g := &errgroup.Group{}
	g.SetLimit(t.ApplyThreads)
	for _, result := range t.BatchResults {
		valArgs := result
		g.Go(func() error {
			query := common.StringsBuilder(GenMySQLInsertSQLStmtPrefix(
				t.SyncMeta.SchemaNameT,
				t.SyncMeta.TableNameT,
				t.SourceColumns,
				t.SafeMode), valArgs)
			err := t.MySQL.WriteMySQLTable(query)
			if err != nil {
				return fmt.Errorf("error on write db, sql: [%v], error: %v", query, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("target schema table rowid data applier finished",
		zap.String("schema", t.SyncMeta.SchemaNameT),
		zap.String("table", t.SyncMeta.TableNameT),
		zap.String("rowid", t.SyncMeta.ChunkDetailS),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
