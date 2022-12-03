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
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/module/query/mysql"
	"go.uber.org/zap"
	"time"
)

func filterCFGTable(cfg *config.Config, mysql *mysql.MySQL) ([]string, []string, error) {
	startTime := time.Now()
	ok, err := mysql.IsExistMySQLSchema(cfg.MySQLConfig.SchemaName)
	if err != nil {
		return []string{}, []string{}, err
	}

	if !ok {
		return []string{}, []string{}, fmt.Errorf("filter cfg mysql schema [%v] tables isn't exists", cfg.MySQLConfig.SchemaName)
	}

	// 获取 mysql 所有数据表
	// ToDO: 表过滤 include-table/exclude-table
	normalTables, err := mysql.GetMySQLNormalTable(cfg.MySQLConfig.SchemaName)
	if err != nil {
		return normalTables, []string{}, err
	}

	viewTables, err := mysql.GetMySQLViewTable(cfg.MySQLConfig.SchemaName)
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
