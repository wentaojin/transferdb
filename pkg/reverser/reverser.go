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
package reverser

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"
)

func ReverseOracleToMySQLTable(engine *service.Engine, cfg *service.CfgFile) error {
	startTime := time.Now()
	zap.L().Info("reverse table oracle to mysql start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	// 获取配置文件待同步表列表
	exporterTableSlice, err := cfg.GenerateTables(engine)
	if err != nil {
		return err
	}

	if len(exporterTableSlice) == 0 {
		zap.L().Warn("there are no table objects in the oracle schema",
			zap.String("schema", cfg.SourceConfig.SchemaName))
		return nil
	}

	// 判断 table_error_detail 是否存在错误记录，是否可进行 reverse
	errorTotals, err := engine.GetTableErrorDetailCountByMode(cfg.SourceConfig.SchemaName, utils.ReverseMode)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCountByMode] reverse schema [%s] table mode [%s] task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.ReverseMode, err)
	}
	if errorTotals > 0 {
		return fmt.Errorf("func [GetTableErrorDetailCountByMode] reverse schema [%s] table mode [%s] task failed, table [table_error_detail] exist failed error, please clear and rerunning", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.ReverseMode)
	}

	// oracle db collation
	nlsSort, err := engine.GetOracleDBCharacterNLSSortCollation()
	if err != nil {
		return err
	}
	nlsComp, err := engine.GetOracleDBCharacterNLSCompCollation()
	if err != nil {
		return err
	}
	if _, ok := utils.OracleCollationMap[strings.ToUpper(nlsSort)]; !ok {
		return fmt.Errorf("oracle db nls sort [%s] isn't support", nlsSort)
	}
	if _, ok := utils.OracleCollationMap[strings.ToUpper(nlsComp)]; !ok {
		return fmt.Errorf("oracle db nls comp [%s] isn't support", nlsComp)
	}
	if !strings.EqualFold(nlsSort, nlsComp) {
		return fmt.Errorf("oracle db nls_sort [%s] and nls_comp [%s] isn't different, need be equal; because mysql db isn't support", nlsSort, nlsComp)
	}

	// 表列表
	tables, partitionTableList, temporaryTableList, clusteredTableList, err := LoadOracleToMySQLTableList(
		engine, cfg, exporterTableSlice, nlsSort, nlsComp)
	if err != nil {
		return err
	}

	var (
		pwdDir                         string
		fileReverse, fileCompatibility *os.File
	)
	pwdDir, err = os.Getwd()
	if err != nil {
		return err
	}

	fileReverse, err = os.OpenFile(filepath.Join(pwdDir, fmt.Sprintf("reverse_%s.sql", cfg.SourceConfig.SchemaName)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileReverse.Close()

	fileCompatibility, err = os.OpenFile(filepath.Join(pwdDir, fmt.Sprintf("compatibility_%s.sql", cfg.SourceConfig.SchemaName)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileCompatibility.Close()

	wrReverse := &FileMW{sync.Mutex{}, fileReverse}
	wrComp := &FileMW{sync.Mutex{}, fileCompatibility}

	// 创建 Schema
	if err := GenCreateSchema(wrReverse, engine, strings.ToUpper(cfg.SourceConfig.SchemaName), strings.ToUpper(cfg.TargetConfig.SchemaName), nlsComp); err != nil {
		return err
	}

	// 不兼容项 - 表提示
	if err = CompatibilityDBTips(wrComp, strings.ToUpper(cfg.SourceConfig.SchemaName), partitionTableList, temporaryTableList, clusteredTableList); err != nil {
		return err
	}

	// 设置工作池
	// 设置 goroutine 数
	wg := sync.WaitGroup{}
	ch := make(chan Table, utils.BufferSize)

	go func() {
		for _, t := range tables {
			ch <- t
		}
		close(ch)
	}()

	for c := 0; c < cfg.AppConfig.Threads; c++ {
		wg.Add(1)
		go func(revFileMW, compFileMW *FileMW) {
			defer wg.Done()
			for t := range ch {
				writer, err := NewReverseWriter(t, revFileMW, compFileMW)
				if err != nil {
					if err = t.Engine.GormDB.Create(&service.TableErrorDetail{
						SourceSchemaName: t.SourceSchemaName,
						SourceTableName:  t.SourceTableName,
						RunMode:          utils.ReverseMode,
						InfoSources:      utils.ReverseMode,
						RunStatus:        "Failed",
						Detail:           t.String(),
						Error:            err.Error(),
					}).Error; err != nil {
						zap.L().Error("reverse table oracle to mysql failed",
							zap.String("schema", t.SourceSchemaName),
							zap.String("table", t.SourceTableName),
							zap.Error(
								fmt.Errorf("func [NewReverseWriter] reverse table task failed, detail see [table_error_detail], please rerunning")))
						panic(
							fmt.Errorf("func [NewReverseWriter] reverse table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
					}
					continue
				}
				if err = writer.Reverse(); err != nil {
					if err = t.Engine.GormDB.Create(&service.TableErrorDetail{
						SourceSchemaName: t.SourceSchemaName,
						SourceTableName:  t.SourceTableName,
						RunMode:          utils.ReverseMode,
						InfoSources:      utils.ReverseMode,
						RunStatus:        "Failed",
						Detail:           t.String(),
						Error:            err.Error(),
					}).Error; err != nil {
						zap.L().Error("reverse table oracle to mysql failed",
							zap.String("scheme", t.SourceSchemaName),
							zap.String("table", t.SourceTableName),
							zap.Error(
								fmt.Errorf("func [Reverse] reverse table task failed, detail see [table_error_detail], please rerunning")))
						panic(
							fmt.Errorf("func [Reverse] reverse table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
					}
					continue
				}
			}
		}(wrReverse, wrComp)
	}

	wg.Wait()

	errorTotals, err = engine.GetTableErrorDetailCountByMode(cfg.SourceConfig.SchemaName, utils.ReverseMode)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCountByMode] reverse schema [%s] mode [%s] table task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.ReverseMode, err)
	}

	endTime := time.Now()
	zap.L().Info("reverse", zap.String("create table and index output", filepath.Join(pwdDir,
		fmt.Sprintf("reverse_%s.sql", cfg.SourceConfig.SchemaName))))
	zap.L().Info("compatibility", zap.String("maybe exist compatibility output", filepath.Join(pwdDir,
		fmt.Sprintf("compatibility_%s.sql", cfg.SourceConfig.SchemaName))))
	if errorTotals == 0 {
		zap.L().Info("reverse table oracle to mysql finished",
			zap.Int("table totals", len(tables)),
			zap.Int("table success", len(tables)),
			zap.Int("table failed", int(errorTotals)),
			zap.String("cost", endTime.Sub(startTime).String()))
	} else {
		zap.L().Warn("reverse table oracle to mysql finished",
			zap.Int("table totals", len(tables)),
			zap.Int("table success", len(tables)-int(errorTotals)),
			zap.Int("table failed", int(errorTotals)),
			zap.String("failed tips", "failed detail, please see table [table_error_detail]"),
			zap.String("cost", endTime.Sub(startTime).String()))
	}
	return nil
}
