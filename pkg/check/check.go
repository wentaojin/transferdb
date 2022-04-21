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
package check

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/pkg/reverser"

	"go.uber.org/zap"

	"github.com/wentaojin/transferdb/service"
)

func OracleTableToMySQLMappingCheck(engine *service.Engine, cfg *service.CfgFile) error {
	startTime := time.Now()
	service.Logger.Info("check oracle and mysql table start",
		zap.String("oracleSchema", cfg.SourceConfig.SchemaName),
		zap.String("mysqlSchema", cfg.TargetConfig.SchemaName))

	exporterTableSlice, err := cfg.GenerateTables(engine)
	if err != nil {
		return err
	}

	// 判断 table_error_detail 是否存在错误记录，是否可进行 check
	errorTotals, err := engine.GetTableErrorDetailCountByMode(cfg.SourceConfig.SchemaName, utils.CheckMode)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCountByMode] check schema [%s] mode [%s] table task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.CheckMode, err)
	}
	if errorTotals > 0 {
		return fmt.Errorf("func [GetTableErrorDetailCount] check schema [%s] mode [%s] table task failed, table [table_error_detail] exist failed error, please clear and rerunning", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.CheckMode)
	}

	pwdDir, err := os.Getwd()
	if err != nil {
		return err
	}
	fileCheck, err := os.OpenFile(filepath.Join(pwdDir,
		fmt.Sprintf("check_%s.sql", cfg.SourceConfig.SchemaName)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileCheck.Close()

	fileReverse, err := os.OpenFile(filepath.Join(pwdDir, fmt.Sprintf("reverse_%s.sql", cfg.SourceConfig.SchemaName)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileReverse.Close()

	fileCompatibility, err := os.OpenFile(filepath.Join(pwdDir, fmt.Sprintf("compatibility_%s.sql", cfg.SourceConfig.SchemaName)), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fileCompatibility.Close()

	wrCheck := &reverser.FileMW{
		Mutex:  sync.Mutex{},
		Writer: fileCheck}
	wrReverse := &reverser.FileMW{
		Mutex:  sync.Mutex{},
		Writer: fileReverse,
	}
	wrComp := &reverser.FileMW{
		Mutex:  sync.Mutex{},
		Writer: fileCompatibility,
	}

	// oracle 环境信息
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
	oraDBVersion, err := engine.GetOracleDBVersion()
	if err != nil {
		return err
	}

	oraCollation := false
	if utils.VersionOrdinal(oraDBVersion) >= utils.VersionOrdinal(utils.OracleTableColumnCollationDBVersion) {
		oraCollation = true
	}
	finishTime := time.Now()
	service.Logger.Info("get oracle db character and version finished",
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
		service.Logger.Info("get oracle schema and table collation finished",
			zap.String("schema", cfg.SourceConfig.SchemaName),
			zap.String("db version", oraDBVersion),
			zap.String("db character", characterSet),
			zap.Int("table totals", len(exporterTableSlice)),
			zap.Bool("table collation", oraCollation),
			zap.String("cost", finishTime.Sub(beginTime).String()))
	}

	// 设置工作池
	// 设置 goroutine 数
	wg := sync.WaitGroup{}
	ch := make(chan string, utils.BufferSize)

	go func() {
		for _, t := range exporterTableSlice {
			ch <- t
		}
		close(ch)
	}()

	for c := 0; c < cfg.AppConfig.Threads; c++ {
		wg.Add(1)
		go func(
			sourceSchemaName, targetSchemaName, characterSet, nlsSort, nlsComp string,
			tblCollation map[string]string, schemaCollation string,
			oraCollation bool, engine *service.Engine, cfg *service.CfgFile, wrCheck, wrReverse, weComp *reverser.FileMW) {
			defer wg.Done()
			for t := range ch {
				wr := NewDiffWriter(sourceSchemaName, targetSchemaName, t, characterSet,
					nlsSort, nlsComp, tblCollation, schemaCollation, oraCollation,
					engine, cfg, wrCheck, wrReverse, weComp)
				ok, err := wr.CheckTable()
				if err != nil {
					if err = engine.GormDB.Create(&service.TableErrorDetail{
						SourceSchemaName: strings.ToUpper(sourceSchemaName),
						SourceTableName:  t,
						RunMode:          utils.CheckMode,
						InfoSources:      utils.ReverseMode,
						RunStatus:        "Failed",
						Detail:           wr.String(),
						Error:            err.Error(),
					}).Error; err != nil {
						service.Logger.Error("check table oracle to mysql failed",
							zap.String("schema", strings.ToUpper(sourceSchemaName)),
							zap.String("table", t),
							zap.Error(
								fmt.Errorf("func [CheckTable] check table task failed, detail see [table_error_detail], please rerunning")))
						panic(
							fmt.Errorf("func [CheckTable] check table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
					}
					continue
				}

				if ok {
					if err = wr.DiffTable(); err != nil {
						if err = engine.GormDB.Create(&service.TableErrorDetail{
							SourceSchemaName: strings.ToUpper(sourceSchemaName),
							SourceTableName:  t,
							RunMode:          utils.CheckMode,
							InfoSources:      utils.CheckMode,
							RunStatus:        "Failed",
							Detail:           wr.String(),
							Error:            err.Error(),
						}).Error; err != nil {
							service.Logger.Error("check table oracle to mysql failed",
								zap.String("schema", strings.ToUpper(sourceSchemaName)),
								zap.String("table", t),
								zap.Error(
									fmt.Errorf("func [DiffTable] check table task failed, detail see [table_error_detail], please rerunning")))
							panic(
								fmt.Errorf("func [DiffTable] check table task failed, detail see [table_error_detail], please rerunning, error: %v", err))
						}
						continue
					}
				}
			}

		}(cfg.SourceConfig.SchemaName, cfg.TargetConfig.SchemaName, characterSet, nlsSort, nlsComp, tblCollation, schemaCollation, oraCollation,
			engine, cfg, wrCheck, wrReverse, wrComp)
	}

	wg.Wait()

	checkError, err := engine.GetTableErrorDetailCountBySources(cfg.SourceConfig.SchemaName, utils.CheckMode, utils.CheckMode)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCountBySources] check schema [%s] mode [%s] table task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.CheckMode, err)
	}

	reverseError, err := engine.GetTableErrorDetailCountBySources(cfg.SourceConfig.SchemaName, utils.CheckMode, utils.ReverseMode)
	if err != nil {
		return fmt.Errorf("func [GetTableErrorDetailCountBySources] check schema [%s] mode [%s] table task failed, error: %v", strings.ToUpper(cfg.SourceConfig.SchemaName), utils.CheckMode, err)
	}

	endTime := time.Now()
	service.Logger.Info("check", zap.String("output", filepath.Join(pwdDir, fmt.Sprintf("check_%s.sql", cfg.SourceConfig.SchemaName))))
	if checkError == 0 && reverseError == 0 {
		service.Logger.Info("check table oracle to mysql finished",
			zap.Int("table totals", len(exporterTableSlice)),
			zap.Int("table success", len(exporterTableSlice)),
			zap.Int("table failed", 0),
			zap.String("cost", endTime.Sub(startTime).String()))
	} else {
		if reverseError > 0 {
			service.Logger.Info("reverse", zap.String("create table and index output", filepath.Join(pwdDir,
				fmt.Sprintf("reverse_%s.sql", cfg.SourceConfig.SchemaName))))
			service.Logger.Info("compatibility", zap.String("maybe exist compatibility output", filepath.Join(pwdDir,
				fmt.Sprintf("compatibility_%s.sql", cfg.SourceConfig.SchemaName))))
		}
		service.Logger.Warn("check table oracle to mysql finished",
			zap.Int("table totals", len(exporterTableSlice)),
			zap.Int("table success", len(exporterTableSlice)-int(checkError)-int(reverseError)),
			zap.Int("reverse failed", int(reverseError)),
			zap.Int("check failed", int(checkError)),
			zap.String("failed tips", "failed detail, please see table [table_error_detail]"),
			zap.String("cost", endTime.Sub(startTime).String()))
	}
	return nil
}
