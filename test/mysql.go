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
package main

import (
	"fmt"
	"strings"

	"github.com/wentaojin/transferdb/service"

	"github.com/wentaojin/transferdb/server"
)

func main() {
	mysqlCfg := service.TargetConfig{
		Username:      "root",
		Password:      "tidb",
		Host:          "120.92.86.179",
		Port:          4000,
		ConnectParams: "charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true&tidb_txn_mode='optimistic'",
		MetaSchema:    "db_meta",
	}
	engine, err := server.NewMySQLEngineGeneralDB(mysqlCfg, 300, 300)
	if err != nil {
		fmt.Println(err)
	}

	if err = engine.GormDB.Model(&service.WaitSyncMeta{}).
		Where("source_schema_name = ? AND source_table_name = ? AND sync_mode = ?",
			"SYSBENCH",
			strings.ToUpper("SBTEST2"),
			"ALL").
		Updates(map[string]interface{}{
			"full_global_scn": 129,
			"FullSplitTimes":  100,
			"IsPartition":     "NO",
		}).Error; err != nil {
		panic(engine)
	}

}
