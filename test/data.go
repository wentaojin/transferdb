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
	"flag"
	"fmt"
	"github.com/pingcap/log"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/oracle"
	"go.uber.org/zap"
	"sync"
	"time"
)

var (
	schema           = flag.String("schema", "marvin", "specify schema name")
	tablePrefix      = flag.String("prefix", "marvin", "specify table prefix")
	tableNums        = flag.Int("nums", 10, "specify the table nums")
	tableCounts      = flag.Int("counts", 50000000, "specify the table counts")
	tableConcurrency = flag.Int("concurrency", 256, "specify the table concurrency")
)

func main() {
	flag.Parse()

	oraCfg := config.OracleConfig{
		Username:    "marvin",
		Password:    "marvin",
		Host:        "13.21.143.126",
		Port:        1521,
		ServiceName: "orclpdb1",
	}
	ora, err := oracle.NewOracleDBEngine(context.Background(), oraCfg, *schema)
	if err != nil {
		panic(err)
	}

	startTime := time.Now()
	if err = ClearOraTable(ora, *schema, *tablePrefix, *tableNums); err != nil {
		panic(err)
	}
	log.Info("clear table finished", zap.Int("table nums", *tableNums), zap.String("cost", time.Now().Sub(startTime).String()))

	if err = CreateOraTable(ora, *schema, *tablePrefix, *tableNums); err != nil {
		panic(err)
	}
	log.Info("create table finished", zap.Int("table nums", *tableNums), zap.String("cost", time.Now().Sub(startTime).String()))

	// 随机数生成
	GenRandomDataSQL(ora, *schema, *tablePrefix, *tableNums, *tableCounts, *tableConcurrency)
	log.Info("gen table data finished", zap.Int("table nums", *tableNums), zap.String("cost", time.Now().Sub(startTime).String()))

}

func ClearOraTable(oracle *oracle.Oracle, schemaName string, tablePrefix string, tableNums int) error {
	for i := 0; i <= tableNums; i++ {
		var rows int
		if err := oracle.OracleDB.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME = UPPER('%s%d')", tablePrefix, i)).Scan(&rows); err != nil {
			return err
		}
		if rows > 0 {
			if _, err := oracle.OracleDB.Exec(fmt.Sprintf("DROP TABLE %s.%s%d", schemaName, tablePrefix, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func CreateOraTable(oracle *oracle.Oracle, schemaName string, tablePrefix string, tableNums int) error {
	for i := 0; i <= tableNums; i++ {
		if _, err := oracle.OracleDB.Exec(fmt.Sprintf(`CREATE TABLE %s.%s%d (
	n1 NUMBER,
	tp1 TIMESTAMP,
	tp2 TIMESTAMP ( 3 ))`, schemaName, tablePrefix, i)); err != nil {
			return err
		}
	}
	return nil
}

func GenRandomDataSQL(oracle *oracle.Oracle, schemaName string, tablePrefix string, tableNums, tableCounts, tableConcurrency int) {
	vals := fmt.Sprintf("%v,%v)",
		"SYSTIMESTAMP",
		"SYSTIMESTAMP")

	insertPrefix := fmt.Sprintf("INSERT INTO %s.%s", schemaName, tablePrefix)

	wg := sync.WaitGroup{}
	ch := make(chan string, common.ChannelBufferSize)

	go func() {
		for i := 0; i <= tableNums; i++ {
			for j := 0; j < tableCounts; j++ {
				sql := fmt.Sprintf("%s%d VALUES(%d,%v", insertPrefix, i, j, vals)
				ch <- sql
			}
		}
		close(ch)
	}()

	for c := 0; c < tableConcurrency; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sql := range ch {
				if _, err := oracle.OracleDB.Exec(sql); err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
}
