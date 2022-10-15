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
package m2o

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/pkg/filter"
	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func MySQLReverseToOracleTable(engine *service.Engine, cfg *config.CfgFile) error {
	startTime := time.Now()
	zap.L().Info("reverse table mysql to oracle start",
		zap.String("schema", cfg.MySQLConfig.SchemaName))

	// 获取配置文件待同步表列表
	exporterTableSlice, viewTables, err := filter.FilterCFGMySQLTables(cfg, engine)
	if err != nil {
		return err
	}

	if (len(exporterTableSlice) + len(viewTables)) == 0 {
		zap.L().Warn("there are no table objects in the mysql schema",
			zap.String("schema", cfg.MySQLConfig.SchemaName))
		return nil
	}

	// 判断 table_error_detail 是否存在错误记录，是否可进行 reverse
	errorTotals, err := engine.GetTableErrorDetailCountByMode(cfg.MySQLConfig.SchemaName, utils.ReverseMode)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCountByMode] reverse schema [%s] table mode [%s] task failed, error: %v", strings.ToUpper(cfg.MySQLConfig.SchemaName), utils.ReverseMode, err)
	}
	if errorTotals > 0 {
		return fmt.Errorf("func [GetTableErrorDetailCountByMode] reverse schema [%s] table mode [%s] task failed, table [table_error_detail] exist failed error, please clear and rerunning", strings.ToUpper(cfg.MySQLConfig.SchemaName), utils.ReverseMode)
	}

	var (
		pwdDir                         string
		fileReverse, fileCompatibility *os.File
	)
	pwdDir, err = os.Getwd()
	if err != nil {
		return err
	}

	fileReverse, err = os.OpenFile(filepath.Join(pwdDir, fmt.Sprintf("reverse_%s.sql", cfg.MySQLConfig.SchemaName)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileReverse.Close()

	fileCompatibility, err = os.OpenFile(filepath.Join(pwdDir, fmt.Sprintf("compatibility_%s.sql", cfg.MySQLConfig.SchemaName)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileCompatibility.Close()

	wrReverse := &FileMW{sync.Mutex{}, fileReverse}
	wrComp := &FileMW{sync.Mutex{}, fileCompatibility}

	// reverse 表任务列表
	oracleDBVersion, err := engine.GetOracleDBVersion()
	if err != nil {
		return fmt.Errorf("get oracle db version falied: %v", err)
	}

	// Oracle 12.2 版本及以上，column collation extended 模式检查
	isExtended := false

	if utils.VersionOrdinal(oracleDBVersion) >= utils.VersionOrdinal(utils.OracleTableColumnCollationDBVersion) {
		isExtended, err = engine.GetOracleExtendedMode()
		if err != nil {
			return fmt.Errorf("get oracle version [%s] extended mode failed: %v", oracleDBVersion, err)
		}
	}

	reverseTaskTables, errCompatibility, tableCharSetMap, tableCollationMap, err := CompatibilityPreCheck(oracleDBVersion, isExtended, engine, cfg, exporterTableSlice)
	if err != nil {
		return err
	}

	reverseTables, err := GenOracleToMySQLTableList(oracleDBVersion, isExtended, engine, cfg, reverseTaskTables, tableCharSetMap, tableCollationMap)
	if err != nil {
		return err
	}

	// 不兼容项 - 表提示
	if err = CompatibilityDBTips(wrComp, strings.ToUpper(cfg.MySQLConfig.SchemaName), errCompatibility, viewTables); err != nil {
		return err
	}

	// 设置工作池
	// 设置 goroutine 数
	g := &errgroup.Group{}
	g.SetLimit(cfg.AppConfig.Threads)

	for _, table := range reverseTables {
		t := table
		revFileMW := wrReverse
		compFileMW := wrComp
		g.Go(func() error {
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
					zap.L().Error("reverse table mysql to oracle failed",
						zap.String("schema", t.SourceSchemaName),
						zap.String("table", t.SourceTableName),
						zap.Error(
							fmt.Errorf("func [NewReverseWriter] reverse table task failed, detail see [table_error_detail], please rerunning")))

					return fmt.Errorf("func [NewReverseWriter] reverse table task failed, detail see [table_error_detail], please rerunning, error: %v", err)
				}
				return nil
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
					zap.L().Error("reverse table mysql to oracle failed",
						zap.String("scheme", t.SourceSchemaName),
						zap.String("table", t.SourceTableName),
						zap.Error(
							fmt.Errorf("func [Reverse] reverse table task failed, detail see [table_error_detail], please rerunning")))

					return fmt.Errorf("func [Reverse] reverse table task failed, detail see [table_error_detail], please rerunning, error: %v", err)
				}
				return nil
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	errorTotals, err = engine.GetTableErrorDetailCountByMode(cfg.MySQLConfig.SchemaName, utils.ReverseMode)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCountByMode] reverse schema [%s] mode [%s] table task failed, error: %v", strings.ToUpper(cfg.MySQLConfig.SchemaName), utils.ReverseMode, err)
	}

	endTime := time.Now()
	zap.L().Info("reverse", zap.String("create table and index output", filepath.Join(pwdDir,
		fmt.Sprintf("reverse_%s.sql", cfg.MySQLConfig.SchemaName))))
	zap.L().Info("compatibility", zap.String("maybe exist compatibility output", filepath.Join(pwdDir,
		fmt.Sprintf("compatibility_%s.sql", cfg.MySQLConfig.SchemaName))))
	if errorTotals == 0 {
		zap.L().Info("reverse table mysql to oracle finished",
			zap.Int("table all totals", len(exporterTableSlice)+len(viewTables)),
			zap.Int("table compatibility", len(exporterTableSlice)-len(reverseTables)+len(viewTables)),
			zap.Int("table reverse totals", len(reverseTables)),
			zap.Int("table reverse success", len(reverseTables)),
			zap.Int("table reverse failed", int(errorTotals)),
			zap.String("cost", endTime.Sub(startTime).String()))
	} else {
		zap.L().Warn("reverse table mysql to oracle finished",
			zap.Int("table all totals", len(exporterTableSlice)+len(viewTables)),
			zap.Int("table compatibility", len(exporterTableSlice)-len(reverseTables)+len(viewTables)),
			zap.Int("table reverse totals", len(reverseTables)),
			zap.Int("table reverse success", len(reverseTables)-int(errorTotals)),
			zap.Int("table reverse failed", int(errorTotals)),
			zap.String("failed tips", "failed detail, please see table [table_error_detail]"),
			zap.String("cost", endTime.Sub(startTime).String()))
	}
	return nil

}

func GenOracleToMySQLTableList(oracleDBVersion string, isExtended bool, engine *service.Engine, cfg *config.CfgFile, reverseTaskTable []string, tableCharSetMap, tableCollationMap map[string]string) ([]Table, error) {
	var (
		tables []Table
	)

	sourceSchema := strings.ToUpper(cfg.MySQLConfig.SchemaName)

	beginTime := time.Now()
	defer func() {
		endTime := time.Now()
		zap.L().Info("gen oracle table list finished",
			zap.String("schema", sourceSchema),
			zap.Int("table totals", len(reverseTaskTable)),
			zap.Int("table gens", len(tables)),
			zap.String("cost", endTime.Sub(beginTime).String()))
	}()

	startTime := time.Now()

	partitionTables, err := engine.GetMySQLPartitionTable(cfg.MySQLConfig.SchemaName)
	if err != nil {
		return tables, err
	}

	wg := &sync.WaitGroup{}
	chS := make(chan string, utils.BufferSize)
	chT := make(chan Table, utils.BufferSize)

	c := make(chan struct{})

	// 数据 Append
	go func(done func()) {
		for tbl := range chT {
			tables = append(tables, tbl)
		}
		done()
	}(func() {
		c <- struct{}{}
	})

	// 数据 Product
	go func() {
		for _, t := range reverseTaskTable {
			chS <- t
		}
		close(chS)
	}()

	// 数据处理
	for c := 0; c < cfg.AppConfig.Threads; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ts := range chS {
				tbl := Table{
					MySQLDBType:             cfg.MySQLConfig.DBType,
					OracleDBVersion:         oracleDBVersion,
					OracleExtendedMode:      isExtended,
					SourceSchemaName:        strings.ToUpper(sourceSchema),
					TargetSchemaName:        strings.ToUpper(cfg.OracleConfig.SchemaName),
					SourceTableName:         strings.ToUpper(ts),
					IsPartition:             utils.IsContainString(partitionTables, strings.ToUpper(ts)),
					TargetTableName:         strings.ToUpper(ts),
					SourceTableCharacterSet: tableCharSetMap[ts],
					SourceTableCollation:    tableCollationMap[ts],
					Overwrite:               cfg.MySQLConfig.Overwrite,
					Engine:                  engine,
				}
				chT <- tbl
			}
		}()
	}

	wg.Wait()
	close(chT)
	<-c

	endTime := time.Now()
	zap.L().Info("gen mysql slice table finished",
		zap.String("schema", sourceSchema),
		zap.Int("table totals", len(reverseTaskTable)),
		zap.Int("table gens", len(tables)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return tables, nil
}

func CompatibilityPreCheck(oracleDBVersion string, isExtended bool, engine *service.Engine, cfg *config.CfgFile, exporterTableSlice []string) ([]string, map[string][]map[string]string, map[string]string, map[string]string, error) {
	// MySQL CharacterSet And Collation 过滤检查
	tableCharSetMap := make(map[string]string)
	tableCollationMap := make(map[string]string)

	errCompatibility := make(map[string][]map[string]string)

	var (
		errCompINFO       []map[string]string
		reverseTaskTables []string
	)

	for _, t := range exporterTableSlice {
		// 检查表级别字符集以及排序规则
		characterSet, collation, err := engine.GetMySQLTableCharacterSetAndCollation(cfg.MySQLConfig.SchemaName, t)
		if err != nil {
			return []string{}, errCompatibility, tableCharSetMap, tableCollationMap, fmt.Errorf("get mysql table characterSet and collation falied: %v", err)
		}
		_, okTableCharacterSet := utils.MySQLDBCharacterSetMap[strings.ToUpper(characterSet)]
		_, okTableCollation := utils.MySQLDBCollationMap[strings.ToLower(collation)]

		if !okTableCharacterSet || !okTableCollation {
			errCompINFO = append(errCompINFO, map[string]string{
				"TableCharacterSet":  characterSet,
				"TableCollation":     collation,
				"ColumnCharacterSet": "",
				"ColumnCollation":    "",
				"ColumnType":         ""})
		}

		// 检查表字段级别字符集以及排序规则
		// 如果表级别字符集与字段级别字符集不一样，oracle 不支持
		// 如果 Oracle 版本
		columnsMap, err := engine.GetMySQLTableColumn(cfg.MySQLConfig.SchemaName, t)
		if err != nil {
			return []string{}, errCompatibility, tableCharSetMap, tableCollationMap, fmt.Errorf("get mysql table column characterSet and collation falied: %v", err)
		}

		// 12.2 以下版本没有字段级别 collation，使用 oracledb 实例级别 collation
		// 检查表以及字段级别 collation 是否一致等于 utf8mb4_bin / utf8_bin，不一致则输出
		if utils.VersionOrdinal(oracleDBVersion) < utils.VersionOrdinal(utils.OracleTableColumnCollationDBVersion) {
			for _, rowCol := range columnsMap {
				// 检查字段级别排序规则
				_, ok := utils.MySQLDBCollationMap[strings.ToLower(rowCol["COLLATION_NAME"])]

				if utils.IsContainString(utils.OracleIsNotSupportDataType, rowCol["DATA_TYPE"]) ||
					(!strings.EqualFold(rowCol["CHARACTER_SET_NAME"], "UNKNOWN") && !strings.EqualFold(rowCol["CHARACTER_SET_NAME"], characterSet)) ||
					(!ok && !strings.EqualFold(rowCol["COLLATION_NAME"], "UNKNOWN")) ||
					(ok && !strings.EqualFold(rowCol["COLLATION_NAME"], "utf8mb4_bin")) ||
					(ok && !strings.EqualFold(collation, "utf8mb4_bin")) ||
					(ok && !strings.EqualFold(rowCol["COLLATION_NAME"], "utf8_bin")) ||
					(ok && !strings.EqualFold(collation, "utf8_bin")) {
					errCompINFO = append(errCompINFO, map[string]string{
						"TableCharacterSet":  characterSet,
						"TableCollation":     collation,
						"ColumnCharacterSet": fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"]),
						"ColumnCollation":    fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"]),
						"ColumnType":         fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"]),
					})
				}
			}
		}

		if utils.VersionOrdinal(oracleDBVersion) >= utils.VersionOrdinal(utils.OracleTableColumnCollationDBVersion) {
			for _, rowCol := range columnsMap {
				// 检查字段级别排序规则
				_, ok := utils.MySQLDBCollationMap[strings.ToLower(rowCol["COLLATION_NAME"])]

				if utils.IsContainString(utils.OracleIsNotSupportDataType, rowCol["DATA_TYPE"]) ||
					(!strings.EqualFold(rowCol["CHARACTER_SET_NAME"], "UNKNOWN") && !strings.EqualFold(rowCol["CHARACTER_SET_NAME"], characterSet)) ||
					(!ok && !strings.EqualFold(rowCol["COLLATION_NAME"], "UNKNOWN")) ||
					(!isExtended && !strings.EqualFold(rowCol["COLLATION_NAME"], collation)) ||
					(!isExtended && !strings.EqualFold(rowCol["CHARACTER_SET_NAME"], characterSet)) {
					errCompINFO = append(errCompINFO, map[string]string{
						"TableCharacterSet":  characterSet,
						"TableCollation":     collation,
						"ColumnCharacterSet": fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["CHARACTER_SET_NAME"]),
						"ColumnCollation":    fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["COLLATION_NAME"]),
						"ColumnType":         fmt.Sprintf(`%s@%s`, rowCol["COLUMN_NAME"], rowCol["DATA_TYPE"]),
					})
				}
			}
		}

		if len(errCompINFO) > 0 {
			errCompatibility[strings.ToUpper(t)] = errCompINFO
		}

		_, okErrCharSet := errCompatibility[strings.ToUpper(t)]
		// 筛选过滤不兼容表
		// Skip 当前循环，继续
		if okErrCharSet {
			continue
		}
		tableCharSetMap[strings.ToUpper(t)] = characterSet
		tableCollationMap[strings.ToUpper(t)] = collation
		reverseTaskTables = append(reverseTaskTables, strings.ToUpper(t))
	}
	return reverseTaskTables, errCompatibility, tableCharSetMap, tableCollationMap, nil
}
