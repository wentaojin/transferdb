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
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/oracle"
)

func main() {
	oraCfg := config.OracleConfig{
		Username:      "c##logminer",
		Password:      "logminer",
		Host:          "10.21.123.31",
		Port:          1521,
		ServiceName:   "orclcdb",
		PDBName:       "orclpdb1",
		ConnectParams: "poolMinSessions=10&poolMaxSessions=1000&poolWaitTimeout=60s&poolSessionMaxLifetime=1h&poolSessionTimeout=5m&poolIncrement=1&timezone=Local",
		SessionParams: []string{},
		SchemaName:    "marvin",
		NLSLang:       "AMERICAN_AMERICA.AL32UTF8",
		IncludeTable:  []string{"lmt_ductn"},
		ExcludeTable:  nil,
	}

	ctx := context.Background()
	oracleDB, err := oracle.NewOracleDBEngine(ctx, oraCfg)
	if err != nil {
		panic(err)
	}

	cols, res, err := oracleDB.GetOracleTableRowsData(`select * from marvin.lmt_ductn`, 10)
	if err != nil {
		return
	}
	fmt.Println(cols)
	fmt.Println(res)
}
