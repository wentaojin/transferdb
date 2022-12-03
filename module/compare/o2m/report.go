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
package o2m

import (
	"encoding/json"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/scylladb/go-set/strset"
	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/model"
	"github.com/wentaojin/transferdb/module/compare"
	"github.com/wentaojin/transferdb/module/query/mysql"
	"github.com/wentaojin/transferdb/module/query/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"strings"
)

type DBSummary struct {
	Columns   []string
	StringSet *strset.Set
	Crc32Val  uint32
	Rows      int64
}

type Report struct {
	dataCompareMeta model.DataCompareMeta `json:"data_compare_meta"`
	mysql           *mysql.MySQL          `json:"-"`
	oracle          *oracle.Oracle        `json:"-"`
	onlyCheckRows   bool                  `json:"only_check_rows"`
}

func NewReport(dataCompareMeta model.DataCompareMeta, mysql *mysql.MySQL, oracle *oracle.Oracle, onlyCheckRows bool) *Report {
	return &Report{
		dataCompareMeta: dataCompareMeta,
		mysql:           mysql,
		oracle:          oracle,
		onlyCheckRows:   onlyCheckRows,
	}
}

func (r *Report) GenDBQuery() (oracleQuery string, mysqlQuery string) {
	if r.dataCompareMeta.WhereColumn == "" {
		oracleQuery = common.StringsBuilder(
			"SELECT ", r.dataCompareMeta.SourceColumnInfo, " FROM ", r.dataCompareMeta.SourceSchemaName, ".", r.dataCompareMeta.SourceTableName, " WHERE ", r.dataCompareMeta.WhereRange)

		mysqlQuery = common.StringsBuilder(
			"SELECT ", r.dataCompareMeta.TargetColumnInfo, " FROM ", r.dataCompareMeta.TargetSchemaName, ".", r.dataCompareMeta.TargetTableName, " WHERE ", r.dataCompareMeta.WhereRange)
	} else {
		oracleQuery = common.StringsBuilder(
			"SELECT ", r.dataCompareMeta.SourceColumnInfo, " FROM ", r.dataCompareMeta.SourceSchemaName, ".", r.dataCompareMeta.SourceTableName, " WHERE ", r.dataCompareMeta.WhereRange,
			" ORDER BY ", r.dataCompareMeta.WhereColumn, " DESC")

		mysqlQuery = common.StringsBuilder(
			"SELECT ", r.dataCompareMeta.TargetColumnInfo, " FROM ", r.dataCompareMeta.TargetSchemaName, ".", r.dataCompareMeta.TargetTableName, " WHERE ", r.dataCompareMeta.WhereRange, " ORDER BY ", r.dataCompareMeta.WhereColumn, " DESC")
	}
	return
}

func (r *Report) CheckOracleRows(oracleQuery string) (int64, error) {
	rows, err := r.oracle.GetOracleTableActualRows(oracleQuery)
	if err != nil {
		return rows, err
	}
	return rows, nil
}

func (r *Report) CheckMySQLRows(mysqlQuery string) (int64, error) {
	rows, err := r.mysql.GetMySQLTableActualRows(mysqlQuery)
	if err != nil {
		return rows, err
	}
	return rows, nil
}

func (r *Report) ReportCheckRows(f *compare.File) error {
	oracleQuery, mysqlQuery := r.GenDBQuery()
	g1 := &errgroup.Group{}
	g2 := &errgroup.Group{}
	oracleRowsChan := make(chan int64, 1)
	mysqlRowsChan := make(chan int64, 1)

	g1.Go(func() error {
		rows, err := r.CheckOracleRows(oracleQuery)
		if err != nil {
			return err
		}
		oracleRowsChan <- rows
		return nil
	})

	g2.Go(func() error {
		rows, err := r.CheckMySQLRows(mysqlQuery)
		if err != nil {
			return err
		}
		mysqlRowsChan <- rows
		return nil
	})

	if err := g1.Wait(); err != nil {
		return err
	}
	if err := g2.Wait(); err != nil {
		return err
	}

	oracleRows := <-oracleRowsChan
	mysqlRows := <-mysqlRowsChan

	if oracleRows == mysqlRows {
		zap.L().Info("oracle table chunk diff equal",
			zap.String("oracle schema", r.dataCompareMeta.SourceSchemaName),
			zap.String("mysql schema", r.dataCompareMeta.TargetSchemaName),
			zap.String("oracle table", r.dataCompareMeta.SourceTableName),
			zap.String("mysql table", r.dataCompareMeta.TargetTableName),
			zap.Int64("oracle rows count", oracleRows),
			zap.Int64("mysql rows count", mysqlRows),
			zap.String("oracle sql", oracleQuery),
			zap.String("mysql sql", mysqlQuery))
		return nil
	}

	zap.L().Info("oracle table chunk diff isn't equal",
		zap.String("oracle schema", r.dataCompareMeta.SourceSchemaName),
		zap.String("mysql schema", r.dataCompareMeta.TargetSchemaName),
		zap.String("oracle table", r.dataCompareMeta.SourceTableName),
		zap.String("mysql table", r.dataCompareMeta.TargetTableName),
		zap.Int64("oracle rows count", oracleRows),
		zap.Int64("mysql rows count", mysqlRows),
		zap.String("oracle sql", oracleQuery),
		zap.String("mysql sql", mysqlQuery))

	sw := table.NewWriter()
	sw.SetStyle(table.StyleLight)
	sw.AppendHeader(table.Row{"SOURCE TABLE", "SOURCE TABLE COUNTS", "TARGET TABLE", "TARGET TABLE COUNTS", "RANGE"})
	sw.AppendRows([]table.Row{
		{
			common.StringsBuilder(r.dataCompareMeta.SourceSchemaName, ".", r.dataCompareMeta.SourceTableName),
			oracleRows,
			common.StringsBuilder(r.dataCompareMeta.TargetSchemaName, ".", r.dataCompareMeta.TargetTableName),
			mysqlRows,
			r.dataCompareMeta.WhereRange,
		},
	})

	fixSQLStr := fmt.Sprintf("/* \n\toracle and mysql table range [%s] data rows aren't equal\n */\n", r.dataCompareMeta.WhereRange) + sw.Render() + "\n"

	if _, err := f.CWriteString(fixSQLStr); err != nil {
		return fmt.Errorf("fix sql file write [only-check-rows = true] failed: %v", err.Error())
	}

	return nil
}

func (r *Report) ReportCheckCRC32(f *compare.File) error {
	errORA := &errgroup.Group{}
	errMySQL := &errgroup.Group{}
	oraChan := make(chan DBSummary, 1)
	mysqlChan := make(chan DBSummary, 1)

	oracleQuery, mysqlQuery := r.GenDBQuery()

	errORA.Go(func() error {
		oraColumns, oraStringSet, oraCrc32Val, err := r.oracle.GetOracleDataRowStrings(oracleQuery)
		if err != nil {
			return fmt.Errorf("get oracle data row strings failed: %v", err)
		}
		oraChan <- DBSummary{
			Columns:   oraColumns,
			StringSet: oraStringSet,
			Crc32Val:  oraCrc32Val,
		}
		return nil
	})

	errMySQL.Go(func() error {
		mysqlColumns, mysqlStringSet, mysqlCrc32Val, err := r.mysql.GetMySQLDataRowStrings(mysqlQuery)
		if err != nil {
			return fmt.Errorf("get mysql data row strings failed: %v", err)
		}
		mysqlChan <- DBSummary{
			Columns:   mysqlColumns,
			StringSet: mysqlStringSet,
			Crc32Val:  mysqlCrc32Val,
		}
		return nil
	})

	if err := errORA.Wait(); err != nil {
		return err
	}
	if err := errMySQL.Wait(); err != nil {
		return err
	}

	oraReport := <-oraChan
	mysqlReport := <-mysqlChan

	// 数据相同
	if oraReport.Crc32Val == mysqlReport.Crc32Val {
		zap.L().Info("oracle table chunk diff equal",
			zap.String("oracle schema", r.dataCompareMeta.SourceSchemaName),
			zap.String("mysql schema", r.dataCompareMeta.TargetSchemaName),
			zap.String("oracle table", r.dataCompareMeta.SourceTableName),
			zap.String("mysql table", r.dataCompareMeta.TargetTableName),
			zap.Uint32("oracle crc32 values", oraReport.Crc32Val),
			zap.Uint32("mysql crc32 values", mysqlReport.Crc32Val),
			zap.String("oracle sql", oracleQuery),
			zap.String("mysql sql", mysqlQuery))
		return nil
	}

	zap.L().Info("oracle table chunk diff isn't equal",
		zap.String("oracle schema", r.dataCompareMeta.SourceSchemaName),
		zap.String("mysql schema", r.dataCompareMeta.TargetSchemaName),
		zap.String("oracle table", r.dataCompareMeta.SourceTableName),
		zap.String("mysql table", r.dataCompareMeta.TargetTableName),
		zap.Uint32("oracle crc32 values", oraReport.Crc32Val),
		zap.Uint32("mysql crc32 values", mysqlReport.Crc32Val),
		zap.String("oracle sql", oracleQuery),
		zap.String("mysql sql", mysqlQuery))

	//上游存在，下游存在 Skip
	//上游不存在，下游不存在 Skip
	//上游存在，下游不存在 INSERT 下游
	//上游不存在，下游存在 DELETE 下游

	var fixSQL strings.Builder

	// 判断下游数据是否多
	targetMore := strset.Difference(mysqlReport.StringSet, oraReport.StringSet).List()
	if len(targetMore) > 0 {
		fixSQL.WriteString("/*\n")
		fixSQL.WriteString(fmt.Sprintf(" mysql table [%s.%s] chunk [%s] data rows are more \n", r.dataCompareMeta.TargetSchemaName, r.dataCompareMeta.TargetTableName, r.dataCompareMeta.WhereRange))

		sw := table.NewWriter()
		sw.SetStyle(table.StyleLight)
		sw.AppendHeader(table.Row{"DATABASE", "DATA COUNTS SQL", "CRC32"})
		sw.AppendRows([]table.Row{
			{"ORACLE",
				common.StringsBuilder("SELECT COUNT(1)", " FROM ", r.dataCompareMeta.SourceSchemaName, ".", r.dataCompareMeta.SourceTableName, " WHERE ", r.dataCompareMeta.WhereRange),
				oraReport.Crc32Val},
			{"MySQL", common.StringsBuilder(
				"SELECT COUNT(1)", " FROM ", r.dataCompareMeta.TargetSchemaName, ".", r.dataCompareMeta.SourceTableName, " WHERE ", r.dataCompareMeta.WhereRange),
				mysqlReport.Crc32Val},
		})
		fixSQL.WriteString(fmt.Sprintf("%v\n", sw.Render()))
		fixSQL.WriteString("*/\n")
		deletePrefix := common.StringsBuilder("DELETE FROM ", r.dataCompareMeta.TargetSchemaName, ".", r.dataCompareMeta.SourceTableName, " WHERE ")
		for _, t := range targetMore {
			var whereCond []string

			// 计算字段列个数
			colValues := strings.Split(t, ",")
			if len(mysqlReport.Columns) != len(colValues) {
				return fmt.Errorf("mysql schema [%s] table [%s] column counts [%d] isn't match values counts [%d]",
					r.dataCompareMeta.TargetSchemaName, r.dataCompareMeta.SourceTableName, len(mysqlReport.Columns), len(colValues))
			}
			for i := 0; i < len(mysqlReport.Columns); i++ {
				whereCond = append(whereCond, common.StringsBuilder(mysqlReport.Columns[i], "=", colValues[i]))
			}

			fixSQL.WriteString(fmt.Sprintf("%v;\n", common.StringsBuilder(deletePrefix, exstrings.Join(whereCond, " AND "))))
		}
	}

	// 判断上游数据是否多
	sourceMore := strset.Difference(oraReport.StringSet, mysqlReport.StringSet).List()
	if len(sourceMore) > 0 {
		fixSQL.WriteString("/*\n")
		fixSQL.WriteString(fmt.Sprintf(" mysql table [%s.%s] chunk [%s] data rows are less \n", r.dataCompareMeta.TargetSchemaName, r.dataCompareMeta.SourceTableName, r.dataCompareMeta.WhereRange))

		sw := table.NewWriter()
		sw.SetStyle(table.StyleLight)
		sw.AppendHeader(table.Row{"DATABASE", "DATA COUNTS SQL", "CRC32"})
		sw.AppendRows([]table.Row{
			{"ORACLE",
				common.StringsBuilder("SELECT COUNT(1)", " FROM ", r.dataCompareMeta.SourceSchemaName, ".", r.dataCompareMeta.SourceTableName, " WHERE ", r.dataCompareMeta.WhereRange),
				oraReport.Crc32Val},
			{"MySQL", common.StringsBuilder(
				"SELECT COUNT(1)", " FROM ", r.dataCompareMeta.TargetSchemaName, ".", r.dataCompareMeta.SourceTableName, " WHERE ", r.dataCompareMeta.WhereRange),
				mysqlReport.Crc32Val},
		})
		fixSQL.WriteString(fmt.Sprintf("%v\n", sw.Render()))
		fixSQL.WriteString("*/\n")
		insertPrefix := common.StringsBuilder("INSERT INTO ", r.dataCompareMeta.TargetSchemaName, ".", r.dataCompareMeta.SourceTableName, " (", strings.Join(oraReport.Columns, ","), ") VALUES (")
		for _, s := range sourceMore {
			fixSQL.WriteString(fmt.Sprintf("%v;\n", common.StringsBuilder(insertPrefix, s, ")")))
		}
	}

	// 文件写入
	if fixSQL.String() != "" {
		if _, err := f.CWriteString(fixSQL.String()); err != nil {
			return fmt.Errorf("fix sql file write [only-check-rows = false] failed: %v", err.Error())
		}
	}
	return nil
}

func (r *Report) Report(f *compare.File) error {
	if r.onlyCheckRows {
		return r.ReportCheckRows(f)
	}
	return r.ReportCheckCRC32(f)
}

func (r *Report) String() string {
	jsonStr, _ := json.Marshal(r)
	return string(jsonStr)
}
