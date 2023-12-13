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
package o2t

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strconv"
	"strings"
	"time"
)

type Rows struct {
	Ctx             context.Context
	SyncMeta        meta.FullSyncMeta
	Oracle          *oracle.Oracle
	MySQL           *mysql.MySQL
	Stmt            *sql.Stmt
	SourceDBCharset string
	TargetDBCharset string
	ApplyThreads    int
	CallTimeout     int
	BatchSize       int
	SafeMode        bool
	ColumnNameS     []string
	ReadChannel     chan []map[string]interface{}
	WriteChannel    chan []interface{}
}

func NewRows(ctx context.Context, syncMeta meta.FullSyncMeta,
	oracle *oracle.Oracle, mysql *mysql.MySQL, stmt *sql.Stmt, sourceDBCharset string, targetDBCharset string, applyThreads, batchSize, callTimeout int, safeMode bool,
	columnNameS []string) *Rows {

	readChannel := make(chan []map[string]interface{}, common.ChannelBufferSize)
	writeChannel := make(chan []interface{}, common.ChannelBufferSize)

	return &Rows{
		Ctx:             ctx,
		SyncMeta:        syncMeta,
		Oracle:          oracle,
		MySQL:           mysql,
		Stmt:            stmt,
		SourceDBCharset: sourceDBCharset,
		TargetDBCharset: targetDBCharset,
		ApplyThreads:    applyThreads,
		SafeMode:        safeMode,
		BatchSize:       batchSize,
		CallTimeout:     callTimeout,
		ColumnNameS:     columnNameS,
		ReadChannel:     readChannel,
		WriteChannel:    writeChannel,
	}
}

func (t *Rows) ReadData() error {
	startTime := time.Now()

	var (
		originQuerySQL string
		execQuerySQL   string
		columnDetailS  string
	)

	convertRaw, err := common.CharsetConvert([]byte(t.SyncMeta.ColumnDetailS), common.CharsetUTF8MB4, t.SourceDBCharset)
	if err != nil {
		return fmt.Errorf("schema [%s] table [%s] column [%s] charset convert failed, %v", t.SyncMeta.SchemaNameS, t.SyncMeta.TableNameS, t.SyncMeta.ColumnDetailS, err)
	}
	columnDetailS = string(convertRaw)

	switch {
	case strings.EqualFold(t.SyncMeta.ConsistentRead, "YES") && strings.EqualFold(t.SyncMeta.SQLHint, ""):
		originQuerySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` AS OF SCN `, strconv.FormatUint(t.SyncMeta.GlobalScnS, 10), ` WHERE `, t.SyncMeta.ChunkDetailS)
		execQuerySQL = common.StringsBuilder(`SELECT `, columnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` AS OF SCN `, strconv.FormatUint(t.SyncMeta.GlobalScnS, 10), ` WHERE `, t.SyncMeta.ChunkDetailS)
	case strings.EqualFold(t.SyncMeta.ConsistentRead, "YES") && !strings.EqualFold(t.SyncMeta.SQLHint, ""):
		originQuerySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.SQLHint, ` `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` AS OF SCN `, strconv.FormatUint(t.SyncMeta.GlobalScnS, 10), ` WHERE `, t.SyncMeta.ChunkDetailS)
		execQuerySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.SQLHint, ` `, columnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` AS OF SCN `, strconv.FormatUint(t.SyncMeta.GlobalScnS, 10), ` WHERE `, t.SyncMeta.ChunkDetailS)
	case strings.EqualFold(t.SyncMeta.ConsistentRead, "NO") && !strings.EqualFold(t.SyncMeta.SQLHint, ""):
		originQuerySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.SQLHint, ` `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` WHERE `, t.SyncMeta.ChunkDetailS)
		execQuerySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.SQLHint, ` `, columnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` WHERE `, t.SyncMeta.ChunkDetailS)
	default:
		originQuerySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` WHERE `, t.SyncMeta.ChunkDetailS)
		execQuerySQL = common.StringsBuilder(`SELECT `, columnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` WHERE `, t.SyncMeta.ChunkDetailS)
	}

	zap.L().Info("source schema table chunk rows extractor starting",
		zap.String("schema", t.SyncMeta.SchemaNameS),
		zap.String("table", t.SyncMeta.TableNameS),
		zap.String("chunk", t.SyncMeta.ChunkDetailS),
		zap.String("origin sql", originQuerySQL),
		zap.String("exec sql", execQuerySQL),
		zap.String("startTime", startTime.String()))

	err = t.Oracle.GetOracleTableRowsData(execQuerySQL, t.BatchSize, t.CallTimeout, t.SourceDBCharset, t.TargetDBCharset, t.ReadChannel)
	if err != nil {
		// 通道关闭
		close(t.ReadChannel)
		return fmt.Errorf("source sql [%v] execute failed: %v", execQuerySQL, err)
	}

	endTime := time.Now()
	zap.L().Info("source schema table chunk rows extractor finished",
		zap.String("schema", t.SyncMeta.SchemaNameS),
		zap.String("table", t.SyncMeta.TableNameS),
		zap.String("chunk", t.SyncMeta.ChunkDetailS),
		zap.String("origin sql", originQuerySQL),
		zap.String("exec sql", execQuerySQL),
		zap.String("cost", endTime.Sub(startTime).String()))

	// 通道关闭
	close(t.ReadChannel)

	return nil
}

func (t *Rows) ProcessData() error {
	for dataC := range t.ReadChannel {
		var batchRows []any

		for _, dMap := range dataC {
			// get value order by column
			var (
				rowsTMP []any
			)
			for _, column := range t.ColumnNameS {
				if val, ok := dMap[column]; ok {
					rowsTMP = append(rowsTMP, val)
				}
			}

			if len(rowsTMP) != len(t.ColumnNameS) {
				// 通道关闭
				close(t.WriteChannel)
				return fmt.Errorf("source schema table column counts vs data counts isn't match")
			} else {
				batchRows = append(batchRows, rowsTMP...)
			}
		}

		// 数据输入
		t.WriteChannel <- batchRows
	}

	// 通道关闭
	close(t.WriteChannel)

	return nil
}

func (t *Rows) ApplyData() error {
	startTime := time.Now()

	preArgNums := len(t.ColumnNameS) * t.BatchSize
	g := &errgroup.Group{}
	g.SetLimit(t.ApplyThreads)

	zap.L().Info("target schema table chunk data applier starting",
		zap.String("schema", t.SyncMeta.SchemaNameT),
		zap.String("table", t.SyncMeta.TableNameT),
		zap.String("chunk", t.SyncMeta.ChunkDetailS),
		zap.String("startTime", startTime.String()))

	for dataC := range t.WriteChannel {
		vals := dataC
		g.Go(func() error {
			// prepare exec
			if len(vals) == preArgNums {
				_, err := t.Stmt.ExecContext(t.Ctx, vals...)
				if err != nil {
					return fmt.Errorf("target sql execute failed: %v", err)
				}
			} else {
				bathSize := len(vals) / len(t.ColumnNameS)
				sqlStr01 := GenMySQLTablePrepareStmt(t.SyncMeta.SchemaNameT, t.SyncMeta.TableNameT, t.ColumnNameS, bathSize, t.SafeMode)
				err := t.MySQL.WriteMySQLTable(sqlStr01, vals...)
				if err != nil {
					return fmt.Errorf("target sql execute failed: %v", err)
				}
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
