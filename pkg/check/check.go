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
	"sync"
	"time"

	"github.com/xxjwxc/gowp/workpool"

	"go.uber.org/zap"

	"github.com/wentaojin/transferdb/service"
)

func CheckOracleTableToMySQLMapping(engine *service.Engine, cfg *service.CfgFile) error {
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
	file, err := os.OpenFile(filepath.Join(pwdDir, "transferdb.sql"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	service.Logger.Info("check", zap.String("output", filepath.Join(pwdDir, "transferdb.sql")))

	wr := &FileMW{sync.Mutex{}, file}

	wp := workpool.New(cfg.ReverseConfig.ReverseThreads)

	for _, table := range exporterTableSlice {
		sourceSchemaName := cfg.SourceConfig.SchemaName
		targetSchemaName := cfg.TargetConfig.SchemaName
		tableName := table
		e := engine
		fileMW := wr
		wp.Do(func() error {
			if err := NewDiffWriter(sourceSchemaName, targetSchemaName, tableName, e, fileMW).DiffOracleAndMySQLTable(); err != nil {
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
			zap.Error(fmt.Errorf("reverse table task failed, please clear and rerunning")))
		return fmt.Errorf("reverse table task failed, please clear and rerunning")
	}
	service.Logger.Info("check table oracle to mysql finished",
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}
