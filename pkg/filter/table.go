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
package filter

import (
	"fmt"
	"time"

	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
	"go.uber.org/zap"
)

// 过滤配置文件表  O2M
func FilterCFGOracleTables(cfg *config.CfgFile, engine *service.Engine) ([]string, error) {
	startTime := time.Now()
	var (
		exporterTableSlice []string
		excludeTables      []string
		err                error
	)
	err = engine.IsExistOracleSchema(cfg.OracleConfig.SchemaName)
	if err != nil {
		return []string{}, err
	}

	// 获取 oracle 所有数据表
	allTables, err := engine.GetOracleTable(cfg.OracleConfig.SchemaName)
	if err != nil {
		return exporterTableSlice, err
	}

	switch {
	case len(cfg.OracleConfig.IncludeTable) != 0 && len(cfg.OracleConfig.ExcludeTable) == 0:
		// 过滤规则加载
		f, err := Parse(cfg.OracleConfig.IncludeTable)
		if err != nil {
			panic(err)
		}

		for _, t := range allTables {
			if f.MatchTable(t) {
				exporterTableSlice = append(exporterTableSlice, t)
			}
		}
	case len(cfg.OracleConfig.IncludeTable) == 0 && len(cfg.OracleConfig.ExcludeTable) != 0:
		// 过滤规则加载
		f, err := Parse(cfg.OracleConfig.ExcludeTable)
		if err != nil {
			panic(err)
		}

		for _, t := range allTables {
			if f.MatchTable(t) {
				excludeTables = append(excludeTables, t)
			}
		}
		exporterTableSlice = utils.FilterDifferenceStringItems(allTables, excludeTables)

	case len(cfg.OracleConfig.IncludeTable) == 0 && len(cfg.OracleConfig.ExcludeTable) == 0:
		exporterTableSlice = allTables

	default:
		return exporterTableSlice, fmt.Errorf("source config params include-table/exclude-table cannot exist at the same time")
	}

	if len(exporterTableSlice) == 0 {
		return exporterTableSlice, fmt.Errorf("exporter tables aren't exist, please check config params include-table/exclude-table")
	}

	endTime := time.Now()
	zap.L().Info("get oracle to mysql all tables",
		zap.String("schema", cfg.OracleConfig.SchemaName),
		zap.Strings("exporter tables list", exporterTableSlice),
		zap.Int("include table counts", len(exporterTableSlice)),
		zap.Int("exclude table counts", len(excludeTables)),
		zap.Int("all table counts", len(allTables)),
		zap.String("cost", endTime.Sub(startTime).String()))

	return exporterTableSlice, nil
}

// 过滤配置文件表 M2O
func FilterCFGMySQLTables(cfg *config.CfgFile, engine *service.Engine) ([]string, []string, error) {
	startTime := time.Now()
	ok, err := engine.IsExistMySQLSchema(cfg.MySQLConfig.SchemaName)
	if err != nil {
		return []string{}, []string{}, err
	}

	if !ok {
		return []string{}, []string{}, fmt.Errorf("filter cfg mysql schema [%v] tables isn't exists", cfg.MySQLConfig.SchemaName)
	}

	// 获取 mysql 所有数据表
	// ToDO: 表过滤 include-table/exclude-table
	normalTables, err := engine.GetMySQLNormalTable(cfg.MySQLConfig.SchemaName)
	if err != nil {
		return normalTables, []string{}, err
	}

	viewTables, err := engine.GetMySQLViewTable(cfg.MySQLConfig.SchemaName)
	if err != nil {
		return normalTables, viewTables, err
	}

	endTime := time.Now()
	zap.L().Info("get mysql to oracle all tables",
		zap.String("schema", cfg.MySQLConfig.SchemaName),
		//zap.Strings("exporter tables list", allTables),
		zap.Int("all table counts", len(normalTables)+len(viewTables)),
		zap.String("cost", endTime.Sub(startTime).String()))
	return normalTables, viewTables, err
}
