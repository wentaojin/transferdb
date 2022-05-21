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
	"flag"
	"fmt"
	"github.com/pingcap/log"
	"github.com/wentaojin/transferdb/server"
	"github.com/wentaojin/transferdb/service"
	"github.com/wentaojin/transferdb/utils"
	"go.uber.org/zap"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	schemaName       = flag.String("schema", "marvin", "specify schema name")
	tablePrefix      = flag.String("prefix", "marvin", "specify table prefix")
	tableNums        = flag.Int("nums", 10, "specify the table nums")
	tableCounts      = flag.Int("counts", 50000000, "specify the table counts")
	tableConcurrency = flag.Int("concurrency", 256, "specify the table concurrency")
)

func main() {
	flag.Parse()

	oraCfg := service.SourceConfig{
		Username:      "marvin",
		Password:      "marvin",
		Host:          "172.16.4.93",
		Port:          1521,
		ServiceName:   "oracl",
		ConnectParams: "poolMinSessions=10&poolMaxSessions=1000&poolWaitTimeout=60s&poolSessionMaxLifetime=1h&poolSessionTimeout=5m&poolIncrement=1&timezone=Local",
		SessionParams: []string{},
		SchemaName:    "marvin",
		IncludeTable:  nil,
		ExcludeTable:  nil,
	}
	sqlDB, err := server.NewOracleDBEngine(oraCfg)
	if err != nil {
		panic(err)
	}

	engine := &service.Engine{
		OracleDB: sqlDB,
	}

	startTime := time.Now()
	if err = ClearOraTable(engine, *schemaName, *tablePrefix, *tableNums); err != nil {
		panic(err)
	}
	log.Info("clear table finished", zap.Int("table nums", *tableNums), zap.String("cost", time.Now().Sub(startTime).String()))

	if err = CreateOraTable(engine, *schemaName, *tablePrefix, *tableNums); err != nil {
		panic(err)
	}
	log.Info("create table finished", zap.Int("table nums", *tableNums), zap.String("cost", time.Now().Sub(startTime).String()))

	// 随机数生成
	GenRandomDataSQL(engine, *schemaName, *tablePrefix, *tableNums, *tableCounts, *tableConcurrency)
	log.Info("gen table data finished", zap.Int("table nums", *tableNums), zap.String("cost", time.Now().Sub(startTime).String()))

	// 收集统计信息
	AnalyzeOraTable(engine, *schemaName, *tablePrefix, *tableNums)
	log.Info("analyze table data finished", zap.Int("table nums", *tableNums), zap.String("cost", time.Now().Sub(startTime).String()))
}

func ClearOraTable(engine *service.Engine, schemaName string, tablePrefix string, tableNums int) error {
	for i := 0; i <= tableNums; i++ {
		var rows int
		if err := engine.OracleDB.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME = UPPER('%s%d')", tablePrefix, i)).Scan(&rows); err != nil {
			return err
		}
		if rows > 0 {
			if _, err := engine.OracleDB.Exec(fmt.Sprintf("DROP TABLE %s.%s%d", schemaName, tablePrefix, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func CreateOraTable(engine *service.Engine, schemaName string, tablePrefix string, tableNums int) error {
	for i := 0; i <= tableNums; i++ {
		if _, err := engine.OracleDB.Exec(fmt.Sprintf(`CREATE TABLE %s.%s%d (
	n1 NUMBER primary key,
	n2 NUMBER ( 2 ) NOT NULL,
	n3 NUMBER ( 6, 2 ),
	vchar1 VARCHAR ( 10 ) DEFAULT 'ty',
	vchar2 VARCHAR ( 3000 ),
	vchar3 VARCHAR2 ( 3000 ),
	char1 CHAR ( 300 ),
	blobc BLOB,
	clobn CLOB,
	ndate DATE,
	fp1 FLOAT ( 2 ),
	fp2 FLOAT,
	flk LONG,
	rw1 RAW ( 10 ),
	rw2 RAW ( 300 ),
	tp1 TIMESTAMP,
	tp2 TIMESTAMP ( 3 ),
	tp4 TIMESTAMP ( 5 ) WITH TIME ZONE)`, schemaName, tablePrefix, i)); err != nil {
			return err
		}
	}
	return nil
}

func GenRandomDataSQL(engine *service.Engine, schemaName string, tablePrefix string, tableNums, tableCounts, tableConcurrency int) {
	vals := fmt.Sprintf("%d,%v,'%s','%s','%s','%s',%v,'%s',%v,%v,%v,'%s',%v,%v,%v,%v,%v)",
		RandInt64(1, 50),
		RandFloat64(1.20, 103.02),
		RandomAscii(8),
		RandChinese(10),
		RandChinese(20),
		RandomAscii(200),
		fmt.Sprintf("UTL_RAW.CAST_To_RAW('%s')", RandomAscii(300)),
		RandomAscii(300),
		"SYSTIMESTAMP",
		RandFloat64(1.20, 103.05),
		RandFloat64(1, 103),
		RandomAscii(10),
		fmt.Sprintf("UTL_RAW.CAST_To_RAW('%s')", RandomAscii(8)),
		fmt.Sprintf("UTL_RAW.CAST_To_RAW('%s')", RandomAscii(8)),
		"SYSTIMESTAMP",
		"SYSTIMESTAMP",
		"SYSTIMESTAMP")

	insertPrefix := fmt.Sprintf("INSERT INTO %s.%s", schemaName, tablePrefix)

	wg := sync.WaitGroup{}
	ch := make(chan string, utils.BufferSize)

	go func() {
		for i := 0; i <= tableNums; i++ {
			for j := 0; j < tableCounts; j++ {
				sql := fmt.Sprintf("%s%d VALUES(%d,%s", insertPrefix, i, j, vals)
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
				if _, err := engine.OracleDB.Exec(sql); err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
}

func AnalyzeOraTable(engine *service.Engine, schemaName string, tablePrefix string, tableNums int) {
	var wg sync.WaitGroup
	for i := 0; i <= tableNums; i++ {
		wg.Add(1)
		go func(tableSuffix int) {
			defer wg.Done()
			if _, err := engine.OracleDB.Exec(fmt.Sprintf("ANALYZE TABLE %s.%s%d ESTIMATE STATISTICS SAMPLE 100 PERCENT", schemaName, tablePrefix, tableSuffix)); err != nil {
				panic(err)
			}
		}(i)
	}
	wg.Wait()
}

/*
	随机数
*/
// 随机小数
func RandFloat64(min, max float64) float64 {
	if min >= max || min == 0 || max == 0 {
		return max
	}

	minStr := strconv.FormatFloat(min, 'f', -1, 64)
	// 不包含小数点
	if strings.Index(minStr, ".") == -1 {
		return max
	}
	multipleNum := len(minStr) - (strings.Index(minStr, ".") + 1)
	multiple := math.Pow10(multipleNum)
	minMult := min * multiple
	maxMult := max * multiple
	randVal := RandInt64(int64(minMult), int64((maxMult)))
	result := float64(randVal) / multiple
	return result
}

// 随机整数
func RandInt64(min, max int64) int64 {
	if min >= max || min == 0 || max == 0 {
		return max
	}
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(max-min+1) + min
}

// 随机中文
func RandChinese(nums int) string {
	a := make([]rune, nums)
	for i := range a {
		a[i] = rune(RandInt64(19968, 40869))
	}
	return string(a)
}

// 随机字符串
const numbers string = "0123456789"
const letters string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
const specials = "~!@#$%^*()_+-=[]{}|;:,./<>?"
const alphanumberic string = letters + numbers
const ascii string = alphanumberic + specials

func Random(n int, chars string) string {
	if n <= 0 {
		return ""
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, n, n)
	l := len(chars)
	for i := 0; i < n; i++ {
		bytes[i] = chars[r.Intn(l)]
	}
	return string(bytes)
}

func RandomAscii(n int) string {
	return Random(n, ascii)
}

// 随机时间戳
func RandomTimestamp(nums int) string {
	msInt, err := strconv.ParseInt(strconv.FormatInt(time.Now().UnixNano()/1e6, 10), 10, 64)
	if err != nil {
		panic(err)
	}
	tm := time.Unix(0, msInt*int64(time.Millisecond))
	var layOut string
	if nums == 0 {
		layOut = "2006-01-02 15:04:05"
	} else {
		var as []string
		for i := 0; i < nums; i++ {
			as = append(as, "0")
		}
		layOut = fmt.Sprintf("2006-01-02 15:04:05.%s", strings.Join(as, ""))
	}
	return tm.Format(layOut)

}
