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
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
)

type Rows struct {
	Ctx          context.Context
	SyncMeta     meta.FullSyncMeta
	Oracle       *oracle.Oracle
	Cfg          *config.Config
	DBCharsetS   string
	DBCharsetT   string
	ColumnNameS  []string
	ReadChannel  chan [][]string
	WriteChannel chan string
}

func NewRows(ctx context.Context, syncMeta meta.FullSyncMeta,
	oracle *oracle.Oracle, cfg *config.Config, columnNameS []string, sourceDBCharset string) *Rows {

	writeChannel := make(chan string, common.ChannelBufferSize)
	readChannel := make(chan [][]string, common.ChannelBufferSize)

	return &Rows{
		Ctx:          ctx,
		SyncMeta:     syncMeta,
		Oracle:       oracle,
		Cfg:          cfg,
		DBCharsetS:   sourceDBCharset,
		DBCharsetT:   common.StringUPPER(cfg.CSVConfig.Charset),
		ColumnNameS:  columnNameS,
		ReadChannel:  readChannel,
		WriteChannel: writeChannel,
	}
}

func (t *Rows) ReadData() error {
	startTime := time.Now()
	var (
		originQuerySQL string
		execQuerySQL   string
		columnDetailS  string
	)

	convertRaw, err := common.CharsetConvert([]byte(t.SyncMeta.ColumnDetailS), common.CharsetUTF8MB4, common.MigrateOracleCharsetStringConvertMapping[common.StringUPPER(t.Cfg.OracleConfig.Charset)])
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

	err = t.Oracle.GetOracleTableRowsDataCSV(execQuerySQL, t.DBCharsetS, t.DBCharsetT, t.Cfg, t.ReadChannel, t.ColumnNameS)
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
		for _, dSlice := range dataC {
			if len(dSlice) != len(t.ColumnNameS) {
				return fmt.Errorf("source schema table column counts vs data counts isn't match")
			} else {
				// csv 文件行数据输入
				t.WriteChannel <- common.StringsBuilder(exstrings.Join(dSlice, t.Cfg.CSVConfig.Separator), t.Cfg.CSVConfig.Terminator)
			}
		}
	}

	// 通道关闭
	close(t.WriteChannel)

	return nil
}

func (t *Rows) ApplyData() error {
	startTime := time.Now()
	// 文件目录判断
	if err := common.PathExist(
		filepath.Join(
			t.Cfg.CSVConfig.OutputDir,
			strings.ToUpper(strings.Trim(t.SyncMeta.SchemaNameS, "\"")),
			strings.ToUpper(strings.Trim(t.SyncMeta.TableNameS, "\"")))); err != nil {
		return err
	}

	fileW, err := os.OpenFile(t.SyncMeta.CSVFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileW.Close()

	// 使用 bufio 来缓存写入文件，以提高效率
	writer := bufio.NewWriterSize(fileW, 4096)
	defer writer.Flush()

	if t.Cfg.CSVConfig.Header {
		if _, err = writer.WriteString(common.StringsBuilder(exstrings.Join(t.ColumnNameS, t.Cfg.CSVConfig.Separator), t.Cfg.CSVConfig.Terminator)); err != nil {
			return fmt.Errorf("failed to write headers: %v", err)
		}
	}

	for dataC := range t.WriteChannel {
		if _, err = writer.WriteString(dataC); err != nil {
			return fmt.Errorf("failed to write data row to csv %w", err)
		}
	}

	endTime := time.Now()
	zap.L().Info("target schema table chunk data applier finished",
		zap.String("schema", t.SyncMeta.SchemaNameT),
		zap.String("table", t.SyncMeta.TableNameT),
		zap.String("chunk", t.SyncMeta.ChunkDetailS),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
