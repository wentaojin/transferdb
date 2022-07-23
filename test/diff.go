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
		SourceTableName:  "MARVIN0",
		SourceColumnInfo: "NVL(FLK,'') AS FLK,TO_CHAR(TP4,'yyyy-MM-dd HH24:mi:ss') AS TP4,VCHAR3,BLOBC,NVL(CLOBN,'') AS CLOBN,RW2,TO_CHAR(TP1,'yyyy-MM-dd HH24:mi:ss') AS TP1,NVL(CHAR1,'') AS CHAR1,VCHAR1,RW1,DECODE(SUBSTR(N2,1,1),'.','0' || N2,N2) AS N2,DECODE(SUBSTR(N3,1,1),'.','0' || N3,N3) AS N3,VCHAR2,TO_CHAR(NDATE,'yyyy-MM-dd HH24:mi:ss') AS NDATE,DECODE(SUBSTR(FP1,1,1),'.','0' || FP1,FP1) AS FP1,DECODE(SUBSTR(FP2,1,1),'.','0' || FP2,FP2) AS FP2,TO_CHAR(TP2,'yyyy-MM-dd HH24:mi:ss') AS TP2,DECODE(SUBSTR(N1,1,1),'.','0' || N1,N1) AS N1",
		TargetColumnInfo: "IFNULL(FLK,'') AS FLK,FROM_UNIXTIME(UNIX_TIMESTAMP(TP4),'%Y-%m-%d %H:%i:%s') AS TP4,VCHAR3,BLOBC,IFNULL(CLOBN,'') AS CLOBN,RW2,FROM_UNIXTIME(UNIX_TIMESTAMP(TP1),'%Y-%m-%d %H:%i:%s') AS TP1,IFNULL(CHAR1,'') AS CHAR1,VCHAR1,RW1,CAST(0 + CAST(N2 AS CHAR) AS CHAR) AS N2,CAST(0 + CAST(N3 AS CHAR) AS CHAR) AS N3,VCHAR2,DATE_FORMAT(NDATE,'%Y-%m-%d %H:%i:%s') AS NDATE,CAST(0 + CAST(FP1 AS CHAR) AS CHAR) AS FP1,CAST(0 + CAST(FP2 AS CHAR) AS CHAR) AS FP2,FROM_UNIXTIME(UNIX_TIMESTAMP(TP2),'%Y-%m-%d %H:%i:%s') AS TP2,CAST(0 + CAST(N1 AS CHAR) AS CHAR) AS N1",
		Range:            " N1 BETWEEN 4350000 AND 4350009",
		NumberColumn:     "N1",
	}

	fixSQL, err := diff.Report("STEVEN", dataDiff, engine)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(fixSQL)
}
