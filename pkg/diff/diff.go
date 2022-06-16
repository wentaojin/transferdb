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
package diff

import (
	"fmt"
	"github.com/thinkeridea/go-extend/exstrings"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/wentaojin/transferdb/pkg/reverser"

	"github.com/scylladb/go-set/strset"
	"github.com/wentaojin/transferdb/pkg/check"

	"github.com/wentaojin/transferdb/utils"

	"go.uber.org/zap"

	"github.com/wentaojin/transferdb/service"
)

func OracleDiffMySQLTable(engine *service.Engine, cfg *service.CfgFile) error {
	startTime := time.Now()
	zap.L().Info("diff table oracle to mysql start",
		zap.String("schema", cfg.SourceConfig.SchemaName))

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oraDBVersion, err := engine.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if utils.VersionOrdinal(oraDBVersion) < utils.VersionOrdinal(utils.OracleSYNCRequireDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using transferdb tools", oraDBVersion)
	}

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

	// 判断 table_error_detail 是否存在错误记录，是否可进行 diff
	errorTotals, err := engine.GetTableErrorDetailCountByMode(cfg.SourceConfig.SchemaName, utils.DiffMode)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCountByMode] reverse schema [%s] table mode [%s] task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.DiffMode, err)
	}
	if errorTotals > 0 {
		return fmt.Errorf("func [GetTableErrorDetailCountByMode] reverse schema [%s] table mode [%s] task failed, table [table_error_detail] exist failed error, please clear and rerunning", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.DiffMode)
	}

	// 判断并记录待同步表列表
	for _, tableName := range exporterTableSlice {
		isExist, err := engine.IsExistWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, tableName, utils.DiffMode)
		if err != nil {
			return err
		}
		if !isExist {
			if err := engine.InitWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, []string{tableName}, utils.DiffMode); err != nil {
				return err
			}
		}
	}

	// 关于全量断点恢复
	if !cfg.DiffConfig.EnableCheckpoint {
		if err := engine.TruncateDataDiffMetaRecord(cfg.TargetConfig.MetaSchema); err != nil {
			return err
		}
		for _, tableName := range exporterTableSlice {
			if err := engine.DeleteWaitSyncTableMetaRecord(cfg.TargetConfig.MetaSchema, cfg.SourceConfig.SchemaName, tableName, utils.DiffMode); err != nil {
				return err
			}
			// 判断并记录待同步表列表
			isExist, err := engine.IsExistWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, tableName, utils.DiffMode)
			if err != nil {
				return err
			}
			if !isExist {
				if err := engine.InitWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, []string{tableName}, utils.DiffMode); err != nil {
					return err
				}
			}
		}
	}

	// 获取等待同步以及未同步完成的表列表
	waitSyncTableMetas, waitSyncTableInfo, err := engine.GetWaitSyncTableMetaRecord(cfg.SourceConfig.SchemaName, utils.DiffMode)
	if err != nil {
		return err
	}

	partSyncTableMetas, partSyncTableInfo, err := engine.GetPartSyncTableMetaRecord(cfg.SourceConfig.SchemaName, utils.DiffMode)
	if err != nil {
		return err
	}
	if len(waitSyncTableMetas) == 0 && len(partSyncTableMetas) == 0 {
		endTime := time.Now()
		zap.L().Info("all oracle table data diff finished",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()))
		return nil
	}

	// 判断能否断点续传
	panicCheckpointTables, err := engine.JudgingCheckpointResume(cfg.SourceConfig.SchemaName, partSyncTableMetas, utils.DiffMode)
	if err != nil {
		return err
	}
	if len(panicCheckpointTables) != 0 {
		endTime := time.Now()
		zap.L().Error("all oracle table data diff error",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Strings("panic tables", panicCheckpointTables))

		return fmt.Errorf("checkpoint isn't consistent, please reruning [enable-checkpoint = fase]")
	}

	// ORACLE 环境信息
	beginTime := time.Now()
	characterSet, err := engine.GetOracleDBCharacterSet()
	if err != nil {
		return err
	}
	if _, ok := utils.OracleDBCharacterSetMap[strings.Split(characterSet, ".")[1]]; !ok {
		return fmt.Errorf("oracle db character set [%v] isn't support", characterSet)
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

	// oracle 版本是否存在 collation
	oraCollation := false
	if utils.VersionOrdinal(oraDBVersion) >= utils.VersionOrdinal(utils.OracleTableColumnCollationDBVersion) {
		oraCollation = true
	}
	finishTime := time.Now()
	zap.L().Info("get oracle db character and version finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("db version", oraDBVersion),
		zap.String("db character", characterSet),
		zap.Int("table totals", len(exporterTableSlice)),
		zap.Bool("table collation", oraCollation),
		zap.String("cost", finishTime.Sub(beginTime).String()))

	var (
		tblCollation    map[string]string
		schemaCollation string
	)

	if oraCollation {
		beginTime = time.Now()
		schemaCollation, err = engine.GetOracleSchemaCollation(strings.ToUpper(cfg.SourceConfig.SchemaName))
		if err != nil {
			return err
		}
		tblCollation, err = engine.GetOracleTableCollation(strings.ToUpper(cfg.SourceConfig.SchemaName), schemaCollation)
		if err != nil {
			return err
		}
		finishTime = time.Now()
		zap.L().Info("get oracle schema and table collation finished",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("db version", oraDBVersion),
			zap.String("db character", characterSet),
			zap.Int("table totals", len(exporterTableSlice)),
			zap.Bool("table collation", oraCollation),
			zap.String("cost", finishTime.Sub(beginTime).String()))
	}

	// fix sql file 文件创建
	var (
		pwdDir     string
		fixSqlFile *os.File
	)
	pwdDir, err = os.Getwd()
	if err != nil {
		return err
	}

	fixSqlFile, err = os.OpenFile(filepath.Join(pwdDir, cfg.DiffConfig.FixSqlFile), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fixSqlFile.Close()

	fixFileMW := &reverser.FileMW{Mutex: sync.Mutex{}, Writer: fixSqlFile}

	// 数据对比
	if err = StartTableDiff(partSyncTableInfo, waitSyncTableInfo, cfg, engine,
		characterSet, nlsComp, tblCollation, schemaCollation, oraCollation, fixFileMW); err != nil {
		return err
	}

	// 错误核对
	errorTotals, err = engine.GetTableErrorDetailDistinctCountByMode(cfg.SourceConfig.SchemaName, utils.DiffMode)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailDistinctCountByMode] diff schema [%s] mode [%s] table task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.DiffMode, err)
	}

	endTime := time.Now()
	zap.L().Info("diff", zap.String("fix sql file output", filepath.Join(pwdDir, cfg.DiffConfig.FixSqlFile)))
	if errorTotals == 0 {
		zap.L().Info("diff table oracle to mysql finished",
			zap.Int("table totals", len(exporterTableSlice)),
			zap.Int("table success", len(exporterTableSlice)),
			zap.Int("table failed", int(errorTotals)),
			zap.String("cost", endTime.Sub(startTime).String()))
	} else {
		zap.L().Warn("diff table oracle to mysql finished",
			zap.Int("table totals", len(exporterTableSlice)),
			zap.Int("table success", len(exporterTableSlice)-int(errorTotals)),
			zap.Int("table failed", int(errorTotals)),
			zap.String("failed tips", "failed detail, please see table [table_error_detail]"),
			zap.String("cost", endTime.Sub(startTime).String()))
	}
	return nil

}

func StartTableDiff(partSyncTableInfo, waitSyncTableInfo []string, cfg *service.CfgFile, engine *service.Engine,
	characterSet string, nlsComp string, tblCollation map[string]string, schemaCollation string, oraCollation bool, fixFileMW *reverser.FileMW) error {
	// 优先存在断点的表同步
	if len(partSyncTableInfo) > 0 {
		if err := startTableDiffByCheckpoint(cfg, engine, partSyncTableInfo, characterSet, nlsComp, tblCollation, schemaCollation, oraCollation, fixFileMW); err != nil {
			return err
		}
	}
	if len(waitSyncTableInfo) > 0 {
		if err := startTableDiffByNormal(cfg, engine, waitSyncTableInfo, characterSet, nlsComp, tblCollation, schemaCollation, oraCollation, fixFileMW); err != nil {
			return err
		}
	}
	return nil
}

// 断点校验
func startTableDiffByCheckpoint(cfg *service.CfgFile, engine *service.Engine, partSyncTableInfo []string,
	characterSet string, nlsComp string, tblCollation map[string]string, schemaCollation string, oraCollation bool, fixFileMW *reverser.FileMW) error {
	// 预检查
	if err := PreDiffCheck(partSyncTableInfo, cfg, engine); err != nil {
		return err
	}

	diffs, err := NewDiff(cfg, engine, partSyncTableInfo, characterSet, nlsComp, tblCollation, schemaCollation, oraCollation)
	if err != nil {
		return err
	}

	// 数据对比
	for _, d := range diffs {
		diffStartTime := time.Now()
		zap.L().Info("diff single table oracle to mysql start",
			zap.String("schema", d.SourceSchema),
			zap.String("table", d.SourceTable),
			zap.String("start time", diffStartTime.String()))

		// 获取对比记录
		diffMetas, err := engine.GetDataDiffMeta(d.SourceSchema, d.SourceTable)
		if err != nil {
			return err
		}

		// 设置工作池
		// 设置 goroutine 数
		wg := sync.WaitGroup{}
		ch := make(chan service.DataDiffMeta, utils.BufferSize)

		go func() {
			for _, meta := range diffMetas {
				ch <- meta
			}
			close(ch)
		}()

		for c := 0; c < cfg.DiffConfig.DiffThreads; c++ {
			wg.Add(1)
			go func(targetSchema string, e *service.Engine, fixFileMW *reverser.FileMW) {
				defer wg.Done()
				for meta := range ch {
					// 数据对比报告
					r, err := Report(strings.ToUpper(targetSchema), meta, e)
					if err != nil {
						if err = e.GormDB.Create(&service.TableErrorDetail{
							SourceSchemaName: meta.SourceSchemaName,
							SourceTableName:  meta.SourceTableName,
							RunMode:          utils.DiffMode,
							InfoSources:      utils.DiffMode,
							RunStatus:        "Failed",
							Detail:           meta.String(),
							Error:            err.Error(),
						}).Error; err != nil {
							zap.L().Error("diff table oracle to mysql failed",
								zap.String("schema", meta.SourceSchemaName),
								zap.String("table", meta.SourceTableName),
								zap.Error(
									fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning")))
							panic(
								fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
						}
						continue
					}

					// fixSQL 文件写入
					if r != "" {
						if _, err = fmt.Fprintln(fixFileMW, r); err != nil {
							if err = e.GormDB.Create(&service.TableErrorDetail{
								SourceSchemaName: meta.SourceSchemaName,
								SourceTableName:  meta.SourceTableName,
								RunMode:          utils.DiffMode,
								InfoSources:      utils.DiffMode,
								RunStatus:        "Failed",
								Detail:           meta.String(),
								Error:            fmt.Sprintf("fix sql file write failed: %v", err.Error()),
							}).Error; err != nil {
								zap.L().Error("diff table oracle to mysql failed",
									zap.String("schema", meta.SourceSchemaName),
									zap.String("table", meta.SourceTableName),
									zap.Error(
										fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning")))
								panic(
									fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
							}
							continue
						}
					}

					// 清理记录
					if err = engine.DeleteDataDiffMeta(meta.SourceSchemaName, meta.SourceTableName, meta.Range); err != nil {
						if err = e.GormDB.Create(&service.TableErrorDetail{
							SourceSchemaName: meta.SourceSchemaName,
							SourceTableName:  meta.SourceTableName,
							RunMode:          utils.DiffMode,
							InfoSources:      utils.DiffMode,
							RunStatus:        "Failed",
							Detail:           meta.String(),
							Error:            fmt.Sprintf("delete [data_diff_meta] record write failed: %v", err.Error()),
						}).Error; err != nil {
							zap.L().Error("diff table oracle to mysql failed",
								zap.String("schema", meta.SourceSchemaName),
								zap.String("table", meta.SourceTableName),
								zap.Error(
									fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning")))
							panic(
								fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
						}
						continue
					}

				}
			}(cfg.TargetConfig.SchemaName, engine, fixFileMW)
		}

		wg.Wait()

		// 更新记录
		if err = engine.ModifyWaitSyncTableMetaRecord(
			cfg.TargetConfig.MetaSchema,
			cfg.SourceConfig.SchemaName, d.SourceTable, utils.DiffMode); err != nil {
			return err
		}

		diffEndTime := time.Now()
		zap.L().Info("diff single table oracle to mysql finished",
			zap.String("schema", d.SourceSchema),
			zap.String("table", d.SourceTable),
			zap.String("cost", diffEndTime.Sub(diffStartTime).String()))
	}
	return nil
}

// 正常校验
func startTableDiffByNormal(cfg *service.CfgFile, engine *service.Engine, waitSyncTableInfo []string,
	characterSet string, nlsComp string, tblCollation map[string]string, schemaCollation string, oraCollation bool, fixFileMW *reverser.FileMW) error {
	// 预检查
	if err := PreDiffCheck(waitSyncTableInfo, cfg, engine); err != nil {
		return err
	}

	// 预切分
	diffs, err := PreSplitChunk(cfg, engine, waitSyncTableInfo, characterSet, nlsComp, tblCollation, schemaCollation, oraCollation)
	if err != nil {
		return err
	}

	// 数据对比
	for _, d := range diffs {
		diffStartTime := time.Now()
		zap.L().Info("diff single table oracle to mysql start",
			zap.String("schema", d.SourceSchema),
			zap.String("table", d.SourceTable),
			zap.String("start time", diffStartTime.String()))

		// 获取对比记录
		diffMetas, err := engine.GetDataDiffMeta(d.SourceSchema, d.SourceTable)
		if err != nil {
			return err
		}

		// 设置工作池
		// 设置 goroutine 数
		wg := sync.WaitGroup{}
		ch := make(chan service.DataDiffMeta, utils.BufferSize)

		go func() {
			for _, meta := range diffMetas {
				ch <- meta
			}
			close(ch)
		}()

		for c := 0; c < cfg.DiffConfig.DiffThreads; c++ {
			wg.Add(1)
			go func(targetSchema string, e *service.Engine, fixFileMW *reverser.FileMW) {
				defer wg.Done()
				for meta := range ch {
					// 数据对比报告
					r, err := Report(strings.ToUpper(targetSchema), meta, e)
					if err != nil {
						if err = e.GormDB.Create(&service.TableErrorDetail{
							SourceSchemaName: meta.SourceSchemaName,
							SourceTableName:  meta.SourceTableName,
							RunMode:          utils.DiffMode,
							InfoSources:      utils.DiffMode,
							RunStatus:        "Failed",
							Detail:           meta.String(),
							Error:            err.Error(),
						}).Error; err != nil {
							zap.L().Error("diff table oracle to mysql failed",
								zap.String("schema", meta.SourceSchemaName),
								zap.String("table", meta.SourceTableName),
								zap.Error(
									fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning")))
							panic(
								fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
						}
						continue
					}

					// fixSQL 文件写入
					if r != "" {
						if _, err = fmt.Fprintln(fixFileMW, r); err != nil {
							if err = e.GormDB.Create(&service.TableErrorDetail{
								SourceSchemaName: meta.SourceSchemaName,
								SourceTableName:  meta.SourceTableName,
								RunMode:          utils.DiffMode,
								InfoSources:      utils.DiffMode,
								RunStatus:        "Failed",
								Detail:           meta.String(),
								Error:            fmt.Sprintf("fix sql file write failed: %v", err.Error()),
							}).Error; err != nil {
								zap.L().Error("diff table oracle to mysql failed",
									zap.String("schema", meta.SourceSchemaName),
									zap.String("table", meta.SourceTableName),
									zap.Error(
										fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning")))
								panic(
									fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
							}
							continue
						}
					}

					// 清理记录
					if err = engine.DeleteDataDiffMeta(meta.SourceSchemaName, meta.SourceTableName, meta.Range); err != nil {
						if err = e.GormDB.Create(&service.TableErrorDetail{
							SourceSchemaName: meta.SourceSchemaName,
							SourceTableName:  meta.SourceTableName,
							RunMode:          utils.DiffMode,
							InfoSources:      utils.DiffMode,
							RunStatus:        "Failed",
							Detail:           meta.String(),
							Error:            fmt.Sprintf("delete [data_diff_meta] record write failed: %v", err.Error()),
						}).Error; err != nil {
							zap.L().Error("diff table oracle to mysql failed",
								zap.String("schema", meta.SourceSchemaName),
								zap.String("table", meta.SourceTableName),
								zap.Error(
									fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning")))
							panic(
								fmt.Errorf("func [Report] diff table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
						}
						continue
					}

				}
			}(cfg.TargetConfig.SchemaName, engine, fixFileMW)
		}

		wg.Wait()

		// 更新记录

		if err = engine.ModifyWaitSyncTableMetaRecord(
			cfg.TargetConfig.MetaSchema,
			cfg.SourceConfig.SchemaName, d.SourceTable, utils.DiffMode); err != nil {
			return err
		}

		diffEndTime := time.Now()
		zap.L().Info("diff single table oracle to mysql finished",
			zap.String("schema", d.SourceSchema),
			zap.String("table", d.SourceTable),
			zap.String("cost", diffEndTime.Sub(diffStartTime).String()))
	}
	return nil
}

func PreDiffCheck(exporterTableSlice []string, cfg *service.CfgFile, engine *service.Engine) error {
	startTime := time.Now()

	// 判断下游是否存在 ORACLE 表
	var tbls []string

	for _, t := range exporterTableSlice {
		tbls = append(tbls, utils.StringsBuilder("'", t, "'"))
	}
	mysqlTables, err := engine.GetMySQLTableName(cfg.TargetConfig.SchemaName, strings.Join(tbls, ","))
	if err != nil {
		return err
	}

	diffItems := utils.FilterDifferenceStringItems(exporterTableSlice, mysqlTables)
	if len(diffItems) != 0 {
		return fmt.Errorf("table [%v] target db tidb isn't exists", diffItems)
	}

	// 表结构检查
	if !cfg.DiffConfig.IgnoreStructCheck {
		if err = check.OracleTableToMySQLMappingCheck(engine, cfg); err != nil {
			return err
		}
		checkError, err := engine.GetTableErrorDetailCountBySources(cfg.SourceConfig.SchemaName, utils.CheckMode, utils.CheckMode)
		if err != nil {
			return fmt.Errorf("func [GetTableErrorDetailCountBySources] check schema [%s] mode [%s] table task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.CheckMode, err)
		}

		reverseError, err := engine.GetTableErrorDetailCountBySources(cfg.SourceConfig.SchemaName, utils.CheckMode, utils.ReverseMode)
		if err != nil {
			return fmt.Errorf("func [GetTableErrorDetailCountBySources] check schema [%s] mode [%s] table task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.CheckMode, err)
		}
		if checkError != 0 || reverseError != 0 {
			return fmt.Errorf("check schema [%s] mode [%s] table task failed, please check log, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.CheckMode, err)
		}
		pwdDir, err := os.Getwd()
		if err != nil {
			return err
		}

		checkFile := filepath.Join(pwdDir, fmt.Sprintf("check_%s.sql", cfg.SourceConfig.SchemaName))
		file, err := os.Open(checkFile)
		if err != nil {
			return err
		}
		defer file.Close()

		fd, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}
		if string(fd) != "" {
			return fmt.Errorf("oracle and mysql table struct isn't equal, please check fixed file [%s]", checkFile)
		}
	}

	endTime := time.Now()
	zap.L().Info("pre check schema oracle to mysql finished",
		zap.String("schema", strings.ToUpper(cfg.SourceConfig.SchemaName)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func PreSplitChunk(cfg *service.CfgFile, engine *service.Engine, exportTableSlice []string, sourceCharacterSet, nlsComp string,
	sourceTableCollation map[string]string,
	sourceSchemaCollation string,
	oracleCollation bool) ([]Diff, error) {
	startTime := time.Now()

	diffs, err := NewDiff(cfg, engine, exportTableSlice, sourceCharacterSet, nlsComp, sourceTableCollation, sourceSchemaCollation, oracleCollation)
	if err != nil {
		return diffs, err
	}

	// 获取 SCN 以及初始化元数据表
	globalSCN, err := engine.GetOracleCurrentSnapshotSCN()
	if err != nil {
		return diffs, err
	}

	// 设置工作池
	// 设置 goroutine 数
	wg := sync.WaitGroup{}
	ch := make(chan Diff, utils.BufferSize)

	go func() {
		for _, d := range diffs {
			ch <- d
		}
		close(ch)
	}()

	for c := 0; c < cfg.DiffConfig.DiffThreads; c++ {
		wg.Add(1)
		go func(workerID int, globalSCN uint64) {
			defer wg.Done()
			for d := range ch {
				if err = d.SplitChunk(workerID, globalSCN); err != nil {
					zap.L().Panic("pre split table chunk failed", zap.String("table", d.String()), zap.Error(err))
					panic(fmt.Errorf("pre split table [%v] chunk failed, failed table detail please see logfile, error: [%v]", d.String(), err))
				}
			}
		}(c, globalSCN)
	}

	wg.Wait()

	endTime := time.Now()
	zap.L().Info("pre split oracle and mysql table chunk finished",
		zap.String("schema", cfg.SourceConfig.SchemaName),
		zap.String("cost", endTime.Sub(startTime).String()))

	return diffs, nil
}

// 数据对比报告
type ReportSummary struct {
	Columns   []string
	StringSet *strset.Set
	Crc32Val  uint32
}

func Report(targetSchema string, dm service.DataDiffMeta, engine *service.Engine) (string, error) {
	var oraQuery, mysqlQuery string
	if dm.NumberColumn == "" {
		oraQuery = utils.StringsBuilder(
			"SELECT ", dm.SourceColumnInfo, " FROM ", dm.SourceSchemaName, ".", dm.SourceTableName, " WHERE ", dm.Range)

		mysqlQuery = utils.StringsBuilder(
			"SELECT ", dm.TargetColumnInfo, " FROM ", targetSchema, ".", dm.SourceTableName, " WHERE ", dm.Range)
	} else {
		oraQuery = utils.StringsBuilder(
			"SELECT ", dm.SourceColumnInfo, " FROM ", dm.SourceSchemaName, ".", dm.SourceTableName, " WHERE ", dm.Range,
			" ORDER BY ", dm.NumberColumn, " DESC")

		mysqlQuery = utils.StringsBuilder(
			"SELECT ", dm.TargetColumnInfo, " FROM ", targetSchema, ".", dm.SourceTableName, " WHERE ", dm.Range,
			" ORDER BY ", dm.NumberColumn, " DESC")
	}

	// 多线程
	errGO := new(errgroup.Group)
	oraChan := make(chan ReportSummary, 1)
	mysqlChan := make(chan ReportSummary, 1)

	errGO.Go(func() error {
		oraColumns, oraStringSet, oraCrc32Val, err := engine.GetOracleDataRowStrings(oraQuery)
		if err != nil {
			return fmt.Errorf("get oracle data row strings failed: %v", err)
		}
		oraChan <- ReportSummary{
			Columns:   oraColumns,
			StringSet: oraStringSet,
			Crc32Val:  oraCrc32Val,
		}
		return nil
	})

	errGO.Go(func() error {
		mysqlColumns, mysqlStringSet, mysqlCrc32Val, err := engine.GetMySQLDataRowStrings(mysqlQuery)
		if err != nil {
			return fmt.Errorf("get mysql data row strings failed: %v", err)
		}
		mysqlChan <- ReportSummary{
			Columns:   mysqlColumns,
			StringSet: mysqlStringSet,
			Crc32Val:  mysqlCrc32Val,
		}
		return nil
	})

	if err := errGO.Wait(); err != nil {
		return "", err
	}

	oraReport := <-oraChan
	mysqlReport := <-mysqlChan

	// 数据相同
	if oraReport.Crc32Val == mysqlReport.Crc32Val {
		zap.L().Info("oracle table chunk diff equal",
			zap.String("oracle schema", dm.SourceSchemaName),
			zap.String("mysql schema", targetSchema),
			zap.String("table", dm.SourceTableName),
			zap.Uint32("oracle crc32 values", oraReport.Crc32Val),
			zap.Uint32("mysql crc32 values", mysqlReport.Crc32Val),
			zap.String("oracle sql", oraQuery),
			zap.String("mysql sql", mysqlQuery))
		return "", nil
	}

	zap.L().Info("oracle table chunk diff isn't equal",
		zap.String("oracle schema", dm.SourceSchemaName),
		zap.String("mysql schema", targetSchema),
		zap.String("table", dm.SourceTableName),
		zap.Uint32("oracle crc32 values", oraReport.Crc32Val),
		zap.Uint32("mysql crc32 values", mysqlReport.Crc32Val),
		zap.String("oracle sql", oraQuery),
		zap.String("mysql sql", mysqlQuery))

	//上游存在，下游存在 Skip
	//上游不存在，下游不存在 Skip
	//上游存在，下游不存在 INSERT 下游
	//上游不存在，下游存在 DELETE 下游

	var fixSQL strings.Builder

	// 判断下游数据是否多
	targetMore := strset.Difference(mysqlReport.StringSet, oraReport.StringSet).List()
	if len(targetMore) > 0 {
		fixSQL.WriteString("/*\n")
		fixSQL.WriteString(fmt.Sprintf(" mysql table [%s.%s] chunk data rows are more \n", targetSchema, dm.SourceTableName))

		sw := table.NewWriter()
		sw.SetStyle(table.StyleLight)
		sw.AppendHeader(table.Row{"DATABASE", "DATA COUNTS SQL", "CRC32"})
		sw.AppendRows([]table.Row{
			{"ORACLE",
				utils.StringsBuilder("SELECT COUNT(1)", " FROM ", dm.SourceSchemaName, ".", dm.SourceTableName, " WHERE ", dm.Range),
				oraReport.Crc32Val},
			{"MySQL", utils.StringsBuilder(
				"SELECT COUNT(1)", " FROM ", targetSchema, ".", dm.SourceTableName, " WHERE ", dm.Range),
				mysqlReport.Crc32Val},
		})
		fixSQL.WriteString(fmt.Sprintf("%v\n", sw.Render()))
		fixSQL.WriteString("*/\n")
		deletePrefix := utils.StringsBuilder("DELETE FROM ", targetSchema, ".", dm.SourceTableName, " WHERE ")
		for _, t := range targetMore {
			var whereCond []string

			// 计算字段列个数
			colValues := strings.Split(t, ",")
			if len(mysqlReport.Columns) != len(colValues) {
				return "", fmt.Errorf("mysql schema [%s] table [%s] column counts [%d] isn't match values counts [%d]",
					targetSchema, dm.SourceTableName, len(mysqlReport.Columns), len(colValues))
			}
			for i := 0; i < len(mysqlReport.Columns); i++ {
				whereCond = append(whereCond, utils.StringsBuilder(mysqlReport.Columns[i], "=", colValues[i]))
			}

			fixSQL.WriteString(fmt.Sprintf("%v;\n", utils.StringsBuilder(deletePrefix, exstrings.Join(whereCond, " AND "))))
		}
	}

	// 判断上游数据是否多
	sourceMore := strset.Difference(oraReport.StringSet, mysqlReport.StringSet).List()
	if len(sourceMore) > 0 {
		fixSQL.WriteString("/*\n")
		fixSQL.WriteString(fmt.Sprintf(" mysql table [%s.%s] chunk data rows are less \n", targetSchema, dm.SourceTableName))

		sw := table.NewWriter()
		sw.SetStyle(table.StyleLight)
		sw.AppendHeader(table.Row{"DATABASE", "DATA COUNTS SQL", "CRC32"})
		sw.AppendRows([]table.Row{
			{"ORACLE",
				utils.StringsBuilder("SELECT COUNT(1)", " FROM ", dm.SourceSchemaName, ".", dm.SourceTableName, " WHERE ", dm.Range),
				oraReport.Crc32Val},
			{"MySQL", utils.StringsBuilder(
				"SELECT COUNT(1)", " FROM ", targetSchema, ".", dm.SourceTableName, " WHERE ", dm.Range),
				mysqlReport.Crc32Val},
		})
		fixSQL.WriteString(fmt.Sprintf("%v\n", sw.Render()))
		fixSQL.WriteString("*/\n")
		insertPrefix := utils.StringsBuilder("INSERT INTO ", targetSchema, ".", dm.SourceTableName, " (", strings.Join(oraReport.Columns, ","), ") VALUES (")
		for _, s := range sourceMore {
			fixSQL.WriteString(fmt.Sprintf("%v;\n", utils.StringsBuilder(insertPrefix, s, ")")))
		}
	}

	return fixSQL.String(), nil
}
