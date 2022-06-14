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
	"github.com/wentaojin/transferdb/pkg/diff"
	"github.com/wentaojin/transferdb/server"
	"github.com/wentaojin/transferdb/service"
)

func main() {
	oraCfg := service.SourceConfig{
		Username:      "c##logminer",
		Password:      "logminer",
		Host:          "172.16.4.93",
		Port:          1521,
		ServiceName:   "orclpdb",
		ConnectParams: "&poolMinSessions=10&poolMaxSessions=1000&poolWaitTimeout=60s&poolSessionMaxLifetime=1h&poolSessionTimeout=5s&poolIncrement=1&timezone=Local",
		SessionParams: []string{},
		SchemaName:    "marvin",
		IncludeTable:  nil,
		ExcludeTable:  nil,
	}
	mysqlCfg := service.TargetConfig{
		Username:      "marvin",
		Password:      "marvin",
		Host:          "172.16.4.93",
		Port:          5500,
		ConnectParams: "charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true&tidb_txn_mode='optimistic'",
		MetaSchema:    "db_meta",
		SchemaName:    "steven",
	}
	oraDB, err := server.NewOracleDBEngine(oraCfg)
	if err != nil {
		fmt.Println(err)
	}

	engine, err := server.NewMySQLEngineGeneralDB(mysqlCfg, 300, 1024)
	if err != nil {
		fmt.Println(err)
	}
	engine.OracleDB = oraDB

	dataDiff := service.DataDiffMeta{
		ID:               0,
		SourceSchemaName: "MARVIN",
		SourceTableName:  "MARVIN2",
		SourceColumnInfo: "DECODE(SUBSTR(FP2,1,1),'.','0' || FP2,FP2) AS FP2",
		TargetColumnInfo: "CAST(0 + CAST(FP2 AS CHAR) AS CHAR) AS FP2",
		Range:            " N1 BETWEEN 1650000 AND 1650001 ",
		NumberColumn:     "N1",
	}

	fixSQL, err := diff.Report("STEVEN", dataDiff, engine)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(fixSQL)
}
