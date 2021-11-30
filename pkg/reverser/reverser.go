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
	"time"

	"github.com/xxjwxc/gowp/workpool"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/wentaojin/transferdb/service"

	"go.uber.org/zap"
)

const (
	// 缓冲通道 size
	bufferSize = 1000
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

	defer func() {
		endTime := time.Now()
		service.Logger.Info("reverse table oracle to mysql finished",
			zap.String("cost", endTime.Sub(startTime).String()))
	}()

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
	// 表列表
	tables, partitionTableList, temporaryTableList, clusteredTableList, err := LoadOracleToMySQLTableList(engine, exporterTableSlice, cfg.SourceConfig.SchemaName, cfg.TargetConfig.SchemaName, cfg.TargetConfig.Overwrite)
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

	if len(partitionTableList) > 0 || len(temporaryTableList) > 0 || len(clusteredTableList) > 0 {
		var builder strings.Builder
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table maybe mysql has compatibility, will convert to normal table, please manual process\n"))
		t := table.NewWriter()
		t.SetStyle(table.StyleLight)
		t.AppendHeader(table.Row{"SCHEMA", "ORACLE TABLE TYPE", "ORACLE", "SUGGEST"})

		if len(partitionTableList) > 0 {
			for _, part := range partitionTableList {
				t.AppendRows([]table.Row{
					{cfg.SourceConfig.SchemaName, "Partition", part, "Manual Process Table"},
				})
			}
		}
		if len(temporaryTableList) > 0 {
			for _, temp := range temporaryTableList {
				t.AppendRows([]table.Row{
					{cfg.SourceConfig.SchemaName, "Temporary", temp, "Manual Process Table"},
				})
			}
		}
		if len(clusteredTableList) > 0 {
			for _, clustered := range clusteredTableList {
				t.AppendRows([]table.Row{
					{cfg.SourceConfig.SchemaName, "Clustered", clustered, "Manual Process Table"},
				})
			}
		}

		builder.WriteString(t.Render() + "\n")
		builder.WriteString("*/\n")
		if _, err = fileCompatibility.WriteString(builder.String()); err != nil {
			return err
		}
	}

	// 创建数据库
	var builder strings.Builder
	builder.WriteString("/*\n")
	builder.WriteString(fmt.Sprintf(" oracle schema reverse mysql database\n"))
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.AppendHeader(table.Row{"#", "ORACLE", "MYSQL", "SUGGEST"})
	t.AppendRows([]table.Row{
		{"Schema", cfg.SourceConfig.SchemaName, cfg.TargetConfig.SchemaName, "Create Schema"},
	})
	builder.WriteString(t.Render() + "\n")
	builder.WriteString("*/\n")
	builder.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;\n", cfg.TargetConfig.SchemaName))
	if _, err = fileReverse.WriteString(builder.String() + "\n"); err != nil {
		return err
	}

	// 设置工作池
	// 设置 goroutine 数
	//wrReverse := &FileMW{sync.Mutex{}, fileReverse}
	//wrComp := &FileMW{sync.Mutex{}, fileCompatibility}
	wp := workpool.New(cfg.AppConfig.Threads)
	jobChan := make(chan Job, bufferSize)
	Produce(wp, tables, jobChan)
	if err = wp.Wait(); err != nil {
		return err
	}
	endTime := time.Now()
	if !wp.IsDone() {
		service.Logger.Error("reverse table oracle to mysql failed",
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Error(fmt.Errorf("reverse table task failed, please rerunning")),
			zap.Error(err))
		return fmt.Errorf("reverse table task failed, please rerunning, error: %v", err)
	}

	// 数据发送完毕，关闭通道
	close(jobChan)

	Consume(fileReverse, fileCompatibility, jobChan)

	service.Logger.Info("reverse", zap.String("create table and index output", filepath.Join(pwdDir,
		fmt.Sprintf("reverse_%s.sql", cfg.SourceConfig.SchemaName))))
	service.Logger.Info("compatibility", zap.String("maybe exist compatibility output", filepath.Join(pwdDir,
		fmt.Sprintf("compatibility_%s.sql", cfg.SourceConfig.SchemaName))))

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
