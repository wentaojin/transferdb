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
	"bufio"
	"context"
	"fmt"
	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Rows struct {
	Ctx          context.Context
	SyncMeta     meta.FullSyncMeta
	Oracle       *oracle.Oracle
	Cfg          *config.Config
	DBCharsetS   string
	DBCharsetT   string
	ColumnNameS  []string
	ReadChannel  chan []map[string]string
	WriteChannel chan string
}

func NewRows(ctx context.Context, syncMeta meta.FullSyncMeta,
	oracle *oracle.Oracle, cfg *config.Config, columnNameS []string, sourceDBCharset string) *Rows {

	writeChannel := make(chan string, common.ChannelBufferSize)
	readChannel := make(chan []map[string]string, common.ChannelBufferSize)

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
	var querySQL string
	switch {
	case strings.EqualFold(t.SyncMeta.ConsistentRead, "YES") && strings.EqualFold(t.SyncMeta.SQLHint, ""):
		querySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` AS OF SCN `, strconv.FormatUint(t.SyncMeta.GlobalScnS, 10), ` WHERE `, t.SyncMeta.ChunkDetailS)
	case strings.EqualFold(t.SyncMeta.ConsistentRead, "YES") && !strings.EqualFold(t.SyncMeta.SQLHint, ""):
		querySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.SQLHint, ` `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` AS OF SCN `, strconv.FormatUint(t.SyncMeta.GlobalScnS, 10), ` WHERE `, t.SyncMeta.ChunkDetailS)
	case strings.EqualFold(t.SyncMeta.ConsistentRead, "NO") && !strings.EqualFold(t.SyncMeta.SQLHint, ""):
		querySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.SQLHint, ` `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` WHERE `, t.SyncMeta.ChunkDetailS)
	default:
		querySQL = common.StringsBuilder(`SELECT `, t.SyncMeta.ColumnDetailS, ` FROM `, t.SyncMeta.SchemaNameS, `.`, t.SyncMeta.TableNameS, ` WHERE `, t.SyncMeta.ChunkDetailS)
	}

	err := t.Oracle.GetOracleTableRowsDataCSV(querySQL, t.DBCharsetS, t.DBCharsetT, t.Cfg, t.ReadChannel)
	if err != nil {
		// 通道关闭
		close(t.ReadChannel)

		return fmt.Errorf("source sql [%v] execute failed: %v", querySQL, err)
	}

	endTime := time.Now()
	zap.L().Info("source schema table chunk rows extractor finished",
		zap.String("schema", t.SyncMeta.SchemaNameS),
		zap.String("table", t.SyncMeta.TableNameS),
		zap.String("chunk", t.SyncMeta.ChunkDetailS),
		zap.String("sql", querySQL),
		zap.String("cost", endTime.Sub(startTime).String()))

	// 通道关闭
	close(t.ReadChannel)

	return nil
}

func (t *Rows) ProcessData() error {

	for dataC := range t.ReadChannel {
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
				return fmt.Errorf("source schema table column counts vs data counts isn't match")
			} else {
				// csv 文件行数据输入
				t.WriteChannel <- common.StringsBuilder(exstrings.Join(rowsTMP, t.Cfg.CSVConfig.Separator), t.Cfg.CSVConfig.Terminator)
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
			strings.ToUpper(t.SyncMeta.SchemaNameS),
			strings.ToUpper(t.SyncMeta.TableNameS))); err != nil {
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
