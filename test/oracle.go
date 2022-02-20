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

	"github.com/wentaojin/transferdb/service"

	"github.com/wentaojin/transferdb/server"
)

func main() {
	oraCfg := service.SourceConfig{
		Username:      "marvin",
		Password:      "marvin",
		Host:          "172.16.4.87",
		Port:          1521,
		ServiceName:   "oratidb",
		ConnectParams: "poolMinSessions=10&poolMaxSessions=1000&poolWaitTimeout=60s&poolSessionMaxLifetime=1h&poolSessionTimeout=5m&poolIncrement=10&timezone=Local",
		SessionParams: []string{"alter session set nls_date_format = 'yyyy-mm-dd hh24:mi:ss'"},
		SchemaName:    "marvin",
		IncludeTable:  nil,
		ExcludeTable:  nil,
	}
	sqlDB, err := server.NewOracleDBEngine(oraCfg)
	if err != nil {
		fmt.Println(err)
	}

	engine := service.Engine{
		OracleDB: sqlDB,
	}

	for i := 0; i < 5500; i++ {
		_, _, err := service.Query(engine.OracleDB, fmt.Sprintf(`CREATE TABLE "MARVIN"."STS%d" (	
	"DNO" VARCHAR2(10) CONSTRAINT "NN_STS_DNO%d" NOT NULL ENABLE, 
	"YEAR" NUMBER(4,0) CONSTRAINT "NN_STS_YEAR%d" NOT NULL ENABLE, 
	"IPNO" NUMBER(2,0), 
	"CPNO" NUMBER(2,0), 
	"NPNO" NUMBER(2,0), 
	"STATE" VARCHAR2(1), 
	"SNO" VARCHAR2(2) CONSTRAINT "NN_STS_SNO%d" NOT NULL ENABLE, 
	"OLD_FLAG" VARCHAR2(1), 
	"VOU_CHECK_FLAG" VARCHAR2(1), 
	 CONSTRAINT "CK_STS_IPNO2%d" CHECK (ipno<=13) ENABLE, 
	 CONSTRAINT "CK_STS_STATE%d" CHECK (state IN ('Y','N')) ENABLE, 
	 CONSTRAINT "CK_STS_OLD_FLAG%d" CHECK (old_flag in  ('Y','N')) ENABLE, 
	 CONSTRAINT "CK_STS_NPNO2%d" CHECK (npno<=13) ENABLE, 
	 CONSTRAINT "CK_STS_NPNO1%d" CHECK (npno>=1) ENABLE, 
	 CONSTRAINT "CK_STS_IPNO1%d" CHECK (ipno>=1) ENABLE, 
	 CONSTRAINT "CK_STS_CPNO2%d" CHECK (cpno<=13) ENABLE, 
	 CONSTRAINT "CK_STS_CPNO1%d" CHECK (cpno>=1) ENABLE)`, i, i, i, i, i, i, i, i, i, i, i, i))
		if err != nil {
			panic(err)
		}

		_, _, err = service.Query(engine.OracleDB, fmt.Sprintf(`CREATE UNIQUE INDEX "MARVIN"."PK_STS%d" ON "MARVIN"."STS%d" ("DNO", "YEAR", "SNO")`, i, i))

		_, _, err = service.Query(engine.OracleDB, fmt.Sprintf(`ALTER TABLE "MARVIN"."STS%d" ADD CONSTRAINT "PK_STS%d" PRIMARY KEY ("DNO", "YEAR", "SNO")`, i, i))
		if err != nil {
			panic(err)
		}
	}
}
