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
	"github.com/wentaojin/transferdb/module/migrate/sql/oracle/o2m"
	"math"

	"github.com/xxjwxc/gowp/workpool"

	"golang.org/x/sync/errgroup"

	"github.com/wentaojin/transferdb/common"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/db_meta?charset=utf8mb4&parseTime=True&loc=Local",
		"root", "sa123456", "172.16.4.81", 5000)

	gormDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Println(err)
	}
	sqlDB, err := gormDB.DB()
	if err != nil {
		fmt.Println(err)
	}

	var (
		arg            []interface{}
		args1, args2   [][]interface{}
		prepareSQL1    string
		prepareSQL2    string
		group1, group2 errgroup.Group
	)
	arg = append(arg, 1)
	arg = append(arg, "gg")
	arg = append(arg, 2)
	arg = append(arg, "gh")
	arg = append(arg, 3)
	arg = append(arg, "gg")
	arg = append(arg, 4)
	arg = append(arg, "gh")
	arg = append(arg, 5)
	arg = append(arg, "gh")

	columnCounts := len([]string{"id", "name"})
	batchSize := 2
	actualBinds := len(arg)
	planBinds := columnCounts * batchSize

	// 向下取整
	splitNums := int(math.Floor(float64(actualBinds) / float64(planBinds)))

	// 计划切分元素位置
	planIntegerBinds := splitNums * planBinds
	// 差值
	differenceBinds := actualBinds - planIntegerBinds

	if differenceBinds == 0 {
		// batch 写入
		// 切分 batch
		args1 = common.SplitMultipleSlice(arg, int64(splitNums))

		// 计算占位符
		rowBatchCounts := actualBinds / columnCounts / splitNums

		prepareSQL1 = common.StringsBuilder(o2m.GenMySQLInsertSQLStmtPrefix("db_meta", "test", []string{"id", "name"}, true),
			o2m.GenMySQLPrepareBindVarStmt(columnCounts, rowBatchCounts))

		fmt.Printf("prepareSQL1: %v\n", prepareSQL1)
		fmt.Printf("prepareArgs1: %v\n", args1)
	} else {
		// batch 写入
		if planIntegerBinds > 0 {
			// 切分 batch
			args1 = common.SplitMultipleSlice(arg[:planIntegerBinds], int64(splitNums))

			// 计算占位符
			rowBatchCounts := planIntegerBinds / columnCounts / splitNums

			prepareSQL1 = common.StringsBuilder(o2m.GenMySQLInsertSQLStmtPrefix("db_meta", "test", []string{"id", "name"}, true),
				o2m.GenMySQLPrepareBindVarStmt(columnCounts, rowBatchCounts))

			fmt.Printf("prepareSQL1: %v\n", prepareSQL1)
			fmt.Printf("prepareArgs1: %v\n", args1)
		}

		// 单次写入
		args2 = append(args2, arg[planIntegerBinds:])
		// 计算占位符
		rowBatchCounts := differenceBinds / columnCounts

		prepareSQL2 = common.StringsBuilder(o2m.GenMySQLInsertSQLStmtPrefix("db_meta", "test", []string{"id", "name"}, true),
			o2m.GenMySQLPrepareBindVarStmt(columnCounts, rowBatchCounts))

		fmt.Printf("prepareSQL2: %v\n", prepareSQL2)
		fmt.Printf("prepareArgs2: %v\n", args2)
	}

	group1.Go(func() error {
		if err = batchWriter(sqlDB, prepareSQL1, args1, 10); err != nil {
			return err
		}
		return nil
	})
	group2.Go(func() error {
		if err = batchWriter(sqlDB, prepareSQL2, args2, 1); err != nil {
			return err
		}
		return nil
	})
	if err = group1.Wait(); err != nil {
		fmt.Printf("group1 error: %v\n", err)
	}
	if err = group2.Wait(); err != nil {
		fmt.Printf("group2 error: %v\n", err)
	}
}

func batchWriter(sqlDB *sql.DB, insertPrepareSql string, args [][]interface{}, applyThreads int) error {
	if len(args) > 0 {
		stmtInsert, err := sqlDB.Prepare(insertPrepareSql)
		if err != nil {
			return err
		}
		defer stmtInsert.Close()

		wp := workpool.New(applyThreads)
		for _, v := range args {
			arg := v
			wp.Do(func() error {
				_, err = stmtInsert.Exec(arg...)
				if err != nil {
					return err
				}
				return nil
			})
		}
		if err = wp.Wait(); err != nil {
			return err
		}
	}
	return nil
}
