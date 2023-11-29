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
	"github.com/thinkeridea/go-extend/exstrings"
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
	SourceDBCharset string
	TargetDBCharset string
	ApplyThreads    int
	BatchSize       int
	SafeMode        bool
	ColumnNameS     []string
	ReadChannel     chan []map[string]string
	WriteChannel    chan string
}

func NewRows(ctx context.Context, syncMeta meta.FullSyncMeta,
	oracle *oracle.Oracle, mysql *mysql.MySQL, sourceDBCharset string, targetDBCharset string, applyThreads, batchSize int, safeMode bool,
	columnNameS []string) *Rows {

	readChannel := make(chan []map[string]string, common.ChannelBufferSize)
	writeChannel := make(chan string, common.ChannelBufferSize)

	return &Rows{
		Ctx:             ctx,
		SyncMeta:        syncMeta,
		Oracle:          oracle,
		MySQL:           mysql,
		SourceDBCharset: sourceDBCharset,
		TargetDBCharset: targetDBCharset,
		ApplyThreads:    applyThreads,
		SafeMode:        safeMode,
		BatchSize:       batchSize,
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

	err = t.Oracle.GetOracleTableRowsData(execQuerySQL, t.BatchSize, t.SourceDBCharset, t.TargetDBCharset, t.ReadChannel)
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

			if len(rowsTMP) != len(t.ColumnNameS) {
				// 通道关闭
				close(t.WriteChannel)

				return fmt.Errorf("source schema table column counts vs data counts isn't match")
			} else {
				batchRows = append(batchRows, common.StringsBuilder("(", exstrings.Join(rowsTMP, ","), ")"))
			}
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
				return fmt.Errorf("target sql [%v] execute failed: %v", querySql, err)
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
