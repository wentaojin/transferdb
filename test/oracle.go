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

	"github.com/wentaojin/transferdb/db"
	"github.com/wentaojin/transferdb/pkg/config"
)

func main() {
	oraCfg := config.SourceConfig{
		Username:      "marvin",
		Password:      "marvin",
		ConnectString: "192.168.2.90:1521/orcl?connect_timeout=2",
		SessionParams: []string{"alter session set nls_date_format = 'yyyy-mm-dd hh24:mi:ss'"},
		SchemaName:    "marvin",
		Timezone:      "UTC",
		IncludeTable:  nil,
		ExcludeTable:  nil,
	}
	sqlDB, err := db.NewOracleDBEngine(oraCfg)
	if err != nil {
		fmt.Println(err)
	}

	engine := db.Engine{
		OracleDB: sqlDB,
	}
	col, res, err := engine.QueryFormatOracleRows(`select sysdate from dual`)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(col)
	fmt.Println(res)

}
