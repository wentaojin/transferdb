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
		ConnectString: "172.16.4.184:8066/orcl?connect_timeout=2",
		SessionParams: []string{"alter session set nls_date_format = 'yyyy-mm-dd hh24:mi:ss'"},
		SchemaName:    "marvin",
		Timezone:      "UTC",
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
	col, res, err := service.Query(engine.OracleDB, `select t.COLUMN_NAME,
	     t.DATA_TYPE,
			 t.CHAR_LENGTH, 
			 NVL(t.CHAR_USED,'UNKNOWN') CHAR_USED,
	     NVL(t.DATA_LENGTH,0) AS DATA_LENGTH,
	     NVL(t.DATA_PRECISION,0) AS DATA_PRECISION,
	     NVL(t.DATA_SCALE,0) AS DATA_SCALE,
	     t.NULLABLE,
	     t.DATA_DEFAULT,
	     c.COMMENTS
	from dba_tab_columns t, dba_col_comments c
	where t.table_name = c.table_name
	 and t.column_name = c.column_name
     and t.owner = c.owner
	 and upper(t.table_name) = upper('unique_test')
	 and upper(t.owner) = upper('marvin')
    order by t.COLUMN_ID`)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(col)
	for _, r := range res {
		if r["DATA_DEFAULT"] == "" || r["COMMENTS"] == "" {
			fmt.Println(r)
		}
	}

}
