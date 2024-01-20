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
	dsn := "root:@tcp(10.12.109.105:4000)/marvin"

	// 打开数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 准备 SQL 语句
	sqlStatement := "INSERT INTO marvin01 (mdate) VALUES (?)"
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
		mdate string
	}{
		{"18\\:49\\:26"},
		{"18\\:49\\:26"},
		{"18\\:49\\:26"},
	} {
		_, err := tx.Stmt(stmt).Exec(data.mdate)
		if err != nil {
			log.Fatal(err)
		}
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	for _, data := range []struct {
		mdate string
	}{
		{"18\\:50\\:26"},
		{"18\\:50\\:26"},
		{"18\\:50\\:26"},
	} {
		_, err = db.Exec("INSERT INTO marvin01 (mdate) VALUES (?)", data.mdate)
		if err != nil {
			log.Fatal(err)
		}
	}

	for _, data := range []struct {
		mdate string
	}{
		{"18\\:52\\:26"},
		{"18\\:52\\:26"},
		{"18\\:52\\:26"},
	} {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO marvin01 (mdate) VALUES ('%v')", data.mdate))
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Batch insert successful.")

}
