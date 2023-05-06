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
	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"time"
)

type Rows struct {
	Ctx          context.Context
	SyncMeta     meta.FullSyncMeta
	Oracle       *oracle.Oracle
	MySQL        *mysql.MySQL
	Meta         *meta.Meta
	ApplyThreads int
	BatchSize    int
	SafeMode     bool
	ColumnNameS  []string
	ReadChannel  chan []map[string]string
	WriteChannel chan string
}

func NewRows(ctx context.Context, syncMeta meta.FullSyncMeta,
	oracle *oracle.Oracle, mysql *mysql.MySQL, meta *meta.Meta, applyThreads, batchSize int, safeMode bool,
	columnNameS []string, readChannel chan []map[string]string, writeChannel chan string) *Rows {

	return &Rows{
		Ctx:          ctx,
		SyncMeta:     syncMeta,
		Oracle:       oracle,
		MySQL:        mysql,
		Meta:         meta,
		ApplyThreads: applyThreads,
		SafeMode:     safeMode,
		BatchSize:    batchSize,
		ColumnNameS:  columnNameS,
		ReadChannel:  readChannel,
		WriteChannel: writeChannel,
	}
}

func (t *Rows) ReadData() error {
	startTime := time.Now()
	querySQL := common.StringsBuilder(`SELECT `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` WHERE `, t.SyncMeta.ChunkDetailS)

	err := t.Oracle.GetOracleTableRowsData(querySQL, t.BatchSize, t.ReadChannel)
	if err != nil {
		// 错误 SQL 记录
		errf := meta.NewChunkErrorDetailModel(t.Meta).CreateChunkErrorDetail(t.Ctx, &meta.ChunkErrorDetail{
			DBTypeS:      t.SyncMeta.DBTypeS,
			DBTypeT:      t.SyncMeta.DBTypeT,
			SchemaNameS:  t.SyncMeta.SchemaNameS,
			TableNameS:   t.SyncMeta.TableNameS,
			SchemaNameT:  t.SyncMeta.SchemaNameT,
			TableNameT:   t.SyncMeta.TableNameT,
			TaskMode:     t.SyncMeta.TaskMode,
			ChunkDetailS: t.SyncMeta.ChunkDetailS,
			InfoDetail:   t.SyncMeta.String(),
			ErrorSQL:     querySQL,
			ErrorDetail:  err.Error(),
		})
		if errf != nil {
			return errf
		}
		return nil
	}

	endTime := time.Now()
	zap.L().Info("source schema table chunk rows extractor finished",
		zap.String("schema", t.SyncMeta.SchemaNameS),
		zap.String("table", t.SyncMeta.TableNameS),
		zap.String("chunk", t.SyncMeta.ChunkDetailS),
		zap.String("sql", querySQL),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func (t *Rows) ProcessData() error {

	for dataC := range t.ReadChannel {
		var batchRows []string

		for _, dMap := range dataC {
			// 按字段名顺序遍历获取对应值
			var (
				rowsTMP []string
			)
			for _, column := range t.ColumnNameS {
				if val, ok := dMap[column]; ok {
					rowsTMP = append(rowsTMP, val)
				}
			}
			batchRows = append(batchRows, common.StringsBuilder("(", exstrings.Join(rowsTMP, ","), ")"))
		}

		// 数据输入
		t.WriteChannel <- common.StringsBuilder(GenMySQLInsertSQLStmtPrefix(
			t.SyncMeta.SchemaNameT,
			t.SyncMeta.TableNameT,
			t.ColumnNameS,
			t.SafeMode), exstrings.Join(batchRows, ","))
	}

	// 通道关闭
	close(t.WriteChannel)

	return nil
}

func (t *Rows) ApplyData() error {
	startTime := time.Now()

	g := &errgroup.Group{}
	g.SetLimit(t.ApplyThreads)

	for dataC := range t.WriteChannel {
		querySql := dataC
		g.Go(func() error {
			err := t.MySQL.WriteMySQLTable(querySql)
			if err != nil {
				// 错误 SQL 记录
				errf := meta.NewChunkErrorDetailModel(t.Meta).CreateChunkErrorDetail(t.Ctx, &meta.ChunkErrorDetail{
					DBTypeS:      t.SyncMeta.DBTypeS,
					DBTypeT:      t.SyncMeta.DBTypeT,
					SchemaNameS:  t.SyncMeta.SchemaNameS,
					TableNameS:   t.SyncMeta.TableNameS,
					SchemaNameT:  t.SyncMeta.SchemaNameT,
					TableNameT:   t.SyncMeta.TableNameT,
					TaskMode:     t.SyncMeta.TaskMode,
					ChunkDetailS: t.SyncMeta.ChunkDetailS,
					InfoDetail:   t.SyncMeta.String(),
					ErrorSQL:     querySql,
					ErrorDetail:  err.Error(),
				})
				if errf != nil {
					return errf
				}
				return nil
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("target schema table chunk data applier finished",
		zap.String("schema", t.SyncMeta.SchemaNameT),
		zap.String("table", t.SyncMeta.TableNameT),
		zap.String("chunk", t.SyncMeta.ChunkDetailS),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}
