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
package check

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/wentaojin/transferdb/zlog"
	"go.uber.org/zap"

	"github.com/wentaojin/transferdb/util"

	"github.com/wentaojin/transferdb/db"
)

type DiffWriter struct {
	Mutex  sync.Mutex
	Writer io.Writer
}

func (d *DiffWriter) Write(b []byte) (n int, err error) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()
	return d.Writer.Write(b)
}

// 表结构 Diff
type DiffMsg struct {
	Msg interface{}
	Err error
}

// 表结构对比
// 以上游 oracle 表结构信息为基准，对比下游 MySQL 表结构
// 1、若上游存在，下游不存在，则输出记录，若上游不存在，下游存在，则默认不输出
// 2、忽略上下游不同索引名、约束名对比，只对比下游是否存在同等约束下同等字段是否存在
// 3、分区只对比分区类型、分区键、分区表达式等，不对比具体每个分区下的情况
func DiffOracleAndMySQLTable(sourceSchemaName, targetSchemaName, tableName string, engine *db.Engine) (string, error) {
	oracleTable, err := NewOracleTableINFO(sourceSchemaName, tableName, engine)
	if err != nil {
		return "", err
	}

	mysqlTable, err := NewMySQLTableINFO(targetSchemaName, tableName, engine)
	if err != nil {
		return "", err
	}

	// 输出格式：表结构一致不输出，只输出上下游不一致信息且输出以下游可执行 SQL 输出
	var builder strings.Builder
	if oracleTable.IsPartition != mysqlTable.IsPartition {
		builder.WriteString(fmt.Sprintf("-- oracle table [%s.%s]", sourceSchemaName, tableName))
		builder.WriteString(fmt.Sprintf("-- mysql table [%s.%s]", targetSchemaName, tableName))
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table type is different from mysql table type\n"))
		builder.WriteString(fmt.Sprintf(" oracle table is partition type [%t]\n", oracleTable.IsPartition))
		builder.WriteString(fmt.Sprintf(" mysql table is partition type [%t]\n", mysqlTable.IsPartition))
		builder.WriteString("*/\n")
		zlog.Logger.Warn("table type different",
			zap.String("oracle table", fmt.Sprintf("%s.%s", sourceSchemaName, tableName)),
			zap.String("mysql table", fmt.Sprintf("%s.%s", targetSchemaName, tableName)),
			zap.String("msg", builder.String()))
		return builder.String(), nil
	}

	if oracleTable.TableComment != mysqlTable.TableComment {
		builder.WriteString("/*\n")
		builder.WriteString(fmt.Sprintf(" oracle table comment [%s]\n", oracleTable.TableComment))
		builder.WriteString(fmt.Sprintf(" mysql table comment [%s]\n", mysqlTable.TableComment))
		builder.WriteString("*/\n")
		builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s COMMENT '%s';\n", targetSchemaName, tableName, oracleTable.TableComment))
	}

	diffPU, isOK := util.IsEqualStruct(oracleTable.PUConstraints, mysqlTable.PUConstraints)
	if !isOK {
		builder.WriteString("/*\n")
		builder.WriteString(" oracle table PK and UK")
		builder.WriteString(" mysql table PK and UK")
		builder.WriteString("*/\n")
		for _, pu := range diffPU {
			value, ok := pu.(ConstraintPUKey)
			if ok {
				if value.ConstraintType
				builder.WriteString(fmt.Sprintf("ALTER TABLE %s.%s COMMENT '%s';\n", targetSchemaName, tableName, oracleTable.TableComment))
			} else {

			}
		}
	}

	diffFK, isOK := util.IsEqualStruct(oracleTable.ForeignConstraints, mysqlTable.ForeignConstraints)

	diffCK, isOK := util.IsEqualStruct(oracleTable.CheckConstraints, mysqlTable.CheckConstraints)

	diffIndex, isOK := util.IsEqualStruct(oracleTable.Indexes, mysqlTable.Indexes)

	diffParts, isOK := util.IsEqualStruct(oracleTable.Partitions, mysqlTable.Partitions)

	return builder.String(), nil
}
