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
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func main() {
	// MySQL 数据库连接信息
	dsn := "root:@tcp(123.912.139.333:4000)/marvin"

	// 打开数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 准备 SQL 语句
	sqlStatement := "INSERT INTO user (name, age) VALUES (?, ?)"
	stmt, err := db.Prepare(sqlStatement)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err != nil {
			// 事务回滚
			tx.Rollback()
		}
	}()

	// 批量插入数据
	for _, data := range []struct {
		Name string
		Age  int
	}{
		{"John", 25},
		{"Alice", 30},
		{"Bob", 22},
	} {
		_, err := tx.Stmt(stmt).Exec(data.Name, data.Age)
		if err != nil {
			log.Fatal(err)
		}
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Batch insert successful.")

}
