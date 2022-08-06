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
	"github.com/wentaojin/transferdb/server"
	"github.com/wentaojin/transferdb/service"
)

func main() {
	mysqlCfg := service.TargetConfig{
		Username:      "root",
		Password:      "",
		Host:          "17.14.4.90",
		Port:          4000,
		ConnectParams: "charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true",
		SchemaName:    "marvin",
	}
	engine, err := server.NewMySQLEngineGeneralDB(mysqlCfg, 300, 300)
	if err != nil {
		fmt.Println(err)
	}

	c, r, err := service.Query(engine.MysqlDB, `select * from marvin.cust_info`)
	if err != nil {
		fmt.Printf("error exec sql,error: %v", err)
	}
	fmt.Println(c)
	fmt.Println(r)
}
