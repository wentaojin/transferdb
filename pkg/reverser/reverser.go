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
	service.Logger.Info("reverse table oracle to mysql start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	// 用于检查下游 MySQL/TiDB 环境检查
	// 只提供表结构转换文本输出，不提供直写下游，故注释下游检查项
	//if err := reverseOracleToMySQLTableInspect(engine, cfg); err != nil {
	//	return err
	//}

	// 获取待转换表
	exporterTableSlice, err := cfg.GenerateTables(engine)
	if err != nil {
		return err
	}
	service.Logger.Info("get oracle to mysql all tables", zap.Strings("tables", exporterTableSlice))

	if len(exporterTableSlice) == 0 {
		service.Logger.Warn("there are no table objects in the oracle schema",
			zap.String("schema", cfg.SourceConfig.SchemaName))
		return nil
	}

	// 判断 table_error_detail 是否存在错误记录，是否可进行 reverse
	errorTotals, err := engine.GetTableErrorDetailCount(cfg.SourceConfig.SchemaName)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCount] reverse schema [%s] table task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), err)
	}
	if errorTotals > 0 {
		return fmt.Errorf("func [GetTableErrorDetailCount] reverse schema [%s] table task failed, table [table_error_detail] exist failed error, please clear and rerunning", strings.ToUpper(cfg.SourceConfig.SchemaName))
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
	if strings.ToUpper(nlsSort) != strings.ToUpper(nlsComp) {
		return fmt.Errorf("oracle db nls_sort [%s] and nls_comp [%s] isn't different, need be equal; because mysql db isn't support", nlsSort, nlsComp)
	}

	// 表列表
	tables, partitionTableList, temporaryTableList, clusteredTableList, err := LoadOracleToMySQLTableList(engine, exporterTableSlice, cfg.SourceConfig.SchemaName, cfg.TargetConfig.SchemaName, nlsSort, nlsComp, cfg.TargetConfig.Overwrite)
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
	ch := make(chan Table, 1024)

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
						Mode:             "Reverse",
						Status:           "Failed",
						Detail:           t.String(),
						Error:            err.Error(),
					}).Error; err != nil {
						service.Logger.Error("reverse table oracle to mysql failed",
							zap.String("scheme", t.SourceSchemaName),
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
						Mode:             "Reverse",
						Status:           "Failed",
						Detail:           t.String(),
						Error:            err.Error(),
					}).Error; err != nil {
						service.Logger.Error("reverse table oracle to mysql failed",
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
	for _, t := range tables {
		ch <- t
	}

	close(ch)
	wg.Wait()

	errorTotals, err = engine.GetTableErrorDetailCount(cfg.SourceConfig.SchemaName)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCount] reverse schema [%s] table task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), err)
	}

	endTime := time.Now()
	service.Logger.Info("reverse", zap.String("create table and index output", filepath.Join(pwdDir,
		fmt.Sprintf("reverse_%s.sql", cfg.SourceConfig.SchemaName))))
	service.Logger.Info("compatibility", zap.String("maybe exist compatibility output", filepath.Join(pwdDir,
		fmt.Sprintf("compatibility_%s.sql", cfg.SourceConfig.SchemaName))))
	if errorTotals == 0 {
		service.Logger.Info("reverse table oracle to mysql finished",
			zap.Int("table totals", len(tables)),
			zap.Int("table success", len(tables)),
			zap.Int("table failed", int(errorTotals)),
			zap.String("cost", endTime.Sub(startTime).String()))
	} else {
		service.Logger.Warn("reverse table oracle to mysql finished",
			zap.Int("table totals", len(tables)),
			zap.Int("table success", len(tables)-int(errorTotals)),
			zap.Int("table failed", int(errorTotals)),
			zap.String("failed tips", "failed detail, please see table [table_error_detail]"),
			zap.String("cost", endTime.Sub(startTime).String()))
	}
	return nil
}

// 表转换前检查
func reverseOracleToMySQLTableInspect(engine *service.Engine, cfg *service.CfgFile) error {
	if err := engine.IsExistOracleSchema(cfg.SourceConfig.SchemaName); err != nil {
		return err
	}
	ok, err := engine.IsExistMySQLSchema(cfg.TargetConfig.SchemaName)
	if err != nil {
		return err
	}
	if !ok {
		_, _, err := service.Query(engine.MysqlDB, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, cfg.TargetConfig.SchemaName))
		if err != nil {
			return err
		}
		return nil
	}

	// 获取 oracle 导出转换表列表
	var exporterTableSlice []string
	if len(cfg.SourceConfig.IncludeTable) != 0 {
		if err := engine.IsExistOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.IncludeTable); err != nil {
			return err
		}
		exporterTableSlice = append(exporterTableSlice, cfg.SourceConfig.IncludeTable...)
	}

	if len(cfg.SourceConfig.ExcludeTable) != 0 {
		exporterTableSlice, err = engine.FilterDifferenceOracleTable(cfg.SourceConfig.SchemaName, cfg.SourceConfig.ExcludeTable)
		if err != nil {
			return err
		}
	}

	// 检查源端 schema 导出表是否存在目标端 schema 内
	existMysqlTables, err := engine.FilterIntersectionMySQLTable(cfg.TargetConfig.SchemaName, exporterTableSlice)
	if err != nil {
		return err
	}
	if len(existMysqlTables) > 0 {
		for _, tbl := range existMysqlTables {
			if cfg.TargetConfig.Overwrite {
				if err := engine.RenameMySQLTableName(cfg.TargetConfig.SchemaName, tbl); err != nil {
					return err
				}
			} else {
				// 表跳过重命名以及创建
				service.Logger.Warn("appear warning",
					zap.String("schema", cfg.TargetConfig.SchemaName),
					zap.String("table", tbl),
					zap.String("warn",
						fmt.Sprintf("config file params overwrite value false, table skip create")))
			}
		}
	}
	return nil
}
