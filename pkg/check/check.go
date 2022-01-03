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

	"github.com/xxjwxc/gowp/workpool"

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
	characterSet, err := engine.GetOracleDBCharacterSet()
	if err != nil {
		return err
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

	var (
		tblCollation    map[string]string
		schemaCollation string
	)

	if oraCollation {
		schemaCollation, err = engine.GetOracleSchemaCollation(strings.ToUpper(cfg.SourceConfig.SchemaName))
		if err != nil {
			return err
		}
		tblCollation, err = engine.GetOracleTableCollation(strings.ToUpper(cfg.SourceConfig.SchemaName))
		if err != nil {
			return err
		}
	}

	wp := workpool.New(cfg.AppConfig.Threads)

	for _, table := range exporterTableSlice {
		sourceSchemaName := cfg.SourceConfig.SchemaName
		targetSchemaName := cfg.TargetConfig.SchemaName
		tableName := table
		e := engine
		checkFile := wrCheck
		revFile := wrReverse
		compFile := wrComp

		wp.Do(func() error {
			if err := NewDiffWriter(sourceSchemaName, targetSchemaName,
				tableName, characterSet, nlsSort, nlsComp,
				tblCollation, schemaCollation, oraCollation,
				e, checkFile, revFile, compFile).DiffOracleAndMySQLTable(); err != nil {
				return err
			}
			return nil
		})
	}
	if err = wp.Wait(); err != nil {
		return err
	}

	endTime := time.Now()
	if !wp.IsDone() {
		service.Logger.Error("check table oracle to mysql failed",
			zap.String("cost", endTime.Sub(startTime).String()),
			zap.Error(fmt.Errorf("check table task failed, please rerunning")),
			zap.Error(err))
		return fmt.Errorf("check table task failed, please rerunning, error: %v", err)
	}

	service.Logger.Info("check", zap.String("output", filepath.Join(pwdDir, fmt.Sprintf("check_%s.sql", cfg.SourceConfig.SchemaName))))
	service.Logger.Info("check table oracle to mysql finished",
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
