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
	"context"
	"fmt"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/mysql"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/check"
	"github.com/wentaojin/transferdb/module/check/o2m"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Task struct {
	ctx             context.Context
	cfg             *config.Config
	tableName       string
	oracleCollation bool
	mysql           *mysql.MySQL
	oracle          *oracle.Oracle
}

func NewPartCompareTableTask(ctx context.Context, cfg *config.Config, compareTables []string, mysql *mysql.MySQL, oracle *oracle.Oracle) []*Task {
	var tasks []*Task
	for _, table := range compareTables {
		tasks = append(tasks, &Task{
			ctx:       ctx,
			cfg:       cfg,
			tableName: table,
			mysql:     mysql,
			oracle:    oracle,
		})
	}
	return tasks
}

func NewWaitCompareTableTask(ctx context.Context,
	cfg *config.Config,
	compareTables []string,
	oracleCollation bool,
	mysql *mysql.MySQL,
	oracle *oracle.Oracle) []*Task {
	var tasks []*Task
	for _, table := range compareTables {
		tasks = append(tasks, &Task{
			ctx:             ctx,
			cfg:             cfg,
			tableName:       table,
			oracleCollation: oracleCollation,
			mysql:           mysql,
			oracle:          oracle,
		})
	}
	return tasks
}

func PreTableStructCheck(ctx context.Context, cfg *config.Config, oracledb *oracle.Oracle, mysqldb *mysql.MySQL, metaDB *meta.Meta, exporters []string) error {
	// 表结构检查
	if !cfg.DiffConfig.IgnoreStructCheck {
		startTime := time.Now()
		oraCfg := cfg.OracleConfig
		oraCfg.IncludeTable = exporters
		err := check.ICheck(o2m.NewO2MCheck(ctx,
			&config.Config{
				AppConfig:    cfg.AppConfig,
				OracleConfig: oraCfg,
				MySQLConfig:  cfg.MySQLConfig,
			},
			oracledb,
			mysqldb,
			metaDB))
		if err != nil {
			return err
		}
		errTotals, err := meta.NewErrorLogDetailModel(metaDB).CountsErrorLogBySchema(ctx, &meta.ErrorLogDetail{
			DBTypeS:     common.TaskDBOracle,
			DBTypeT:     common.TaskDBMySQL,
			SchemaNameS: common.StringUPPER(cfg.OracleConfig.SchemaName),
			RunMode:     common.CheckO2MMode,
		})

		if errTotals != 0 || err != nil {
			return fmt.Errorf("compare schema [%s] mode [%s] table task failed: %v, please check log, error: %v", strings.ToUpper(cfg.OracleConfig.SchemaName), common.CheckO2MMode, err)
		}
		pwdDir, err := os.Getwd()
		if err != nil {
			return err
		}

		checkFile := filepath.Join(pwdDir, fmt.Sprintf("check_%s.sql", cfg.OracleConfig.SchemaName))
		file, err := os.Open(checkFile)
		if err != nil {
			return err
		}
		defer file.Close()

		fd, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		if string(fd) != "" {
			return fmt.Errorf("oracle and mysql table struct isn't equal, please check fixed file [%s]", checkFile)
		}

		endTime := time.Now()
		zap.L().Info("pre check schema oracle to mysql finished",
			zap.String("schema", strings.ToUpper(cfg.OracleConfig.SchemaName)),
			zap.String("cost", endTime.Sub(startTime).String()))
	}

	return nil
}

// 字段查询以 ORACLE 字段为主
// Date/Timestamp 字段类型格式化
// Interval Year/Day 数据字符 TO_CHAR 格式化
func (t *Task) AdjustDBSelectColumn() (sourceColumnInfo string, targetColumnInfo string, err error) {
	var (
		sourceColumnInfos, targetColumnInfos []string
	)
	columnInfo, err := t.oracle.GetOracleSchemaTableColumn(t.cfg.OracleConfig.SchemaName, t.tableName, t.oracleCollation)
	if err != nil {
		return sourceColumnInfo, targetColumnInfo, err
	}

	for _, colsInfo := range columnInfo {
		colName := colsInfo["COLUMN_NAME"]
		switch strings.ToUpper(colsInfo["DATA_TYPE"]) {
		// 数字
		case "NUMBER":
			sourceColumnInfos = append(sourceColumnInfos, common.StringsBuilder("DECODE(SUBSTR(", colName, ",1,1),'.','0' || ", colName, ",", colName, ") AS ", colName))
			targetColumnInfos = append(targetColumnInfos, common.StringsBuilder("CAST(0 + CAST(", colName, " AS CHAR) AS CHAR) AS ", colName))
		case "DECIMAL", "DEC", "DOUBLE PRECISION", "FLOAT", "INTEGER", "INT", "REAL", "NUMERIC", "BINARY_FLOAT", "BINARY_DOUBLE", "SMALLINT":
			sourceColumnInfos = append(sourceColumnInfos, common.StringsBuilder("DECODE(SUBSTR(", colName, ",1,1),'.','0' || ", colName, ",", colName, ") AS ", colName))
			targetColumnInfos = append(targetColumnInfos, common.StringsBuilder("CAST(0 + CAST(", colName, " AS CHAR) AS CHAR) AS ", colName))
		// 字符
		case "BFILE", "CHARACTER", "LONG", "NCHAR VARYING", "ROWID", "UROWID", "VARCHAR", "XMLTYPE", "CHAR", "NCHAR", "NVARCHAR2", "NCLOB", "CLOB":
			sourceColumnInfos = append(sourceColumnInfos, common.StringsBuilder("NVL(", colName, ",'') AS ", colName))
			targetColumnInfos = append(targetColumnInfos, common.StringsBuilder("IFNULL(", colName, ",'') AS ", colName))
		// 二进制
		case "BLOB", "LONG RAW", "RAW":
			sourceColumnInfos = append(sourceColumnInfos, colName)
			targetColumnInfos = append(targetColumnInfos, colName)
		// 时间
		case "DATE":
			sourceColumnInfos = append(sourceColumnInfos, common.StringsBuilder("TO_CHAR(", colName, ",'yyyy-MM-dd HH24:mi:ss') AS ", colName))
			targetColumnInfos = append(targetColumnInfos, common.StringsBuilder("DATE_FORMAT(", colName, ",'%Y-%m-%d %H:%i:%s') AS ", colName))
		// 默认其他类型
		default:
			if strings.Contains(colsInfo["DATA_TYPE"], "INTERVAL") {
				sourceColumnInfos = append(sourceColumnInfos, common.StringsBuilder("TO_CHAR(", colName, ") AS ", colName))
				targetColumnInfos = append(targetColumnInfos, colName)
			} else if strings.Contains(colsInfo["DATA_TYPE"], "TIMESTAMP") {
				sourceColumnInfos = append(sourceColumnInfos, common.StringsBuilder("TO_CHAR(", colName, ",'yyyy-MM-dd HH24:mi:ss') AS ", colName))
				targetColumnInfos = append(targetColumnInfos, common.StringsBuilder("FROM_UNIXTIME(UNIX_TIMESTAMP(", colName, "),'%Y-%m-%d %H:%i:%s') AS ", colName))
			} else {
				sourceColumnInfos = append(sourceColumnInfos, colName)
				targetColumnInfos = append(targetColumnInfos, colName)
			}
		}
	}

	sourceColumnInfo = strings.Join(sourceColumnInfos, ",")
	targetColumnInfo = strings.Join(targetColumnInfos, ",")

	return sourceColumnInfo, targetColumnInfo, nil
}

// 筛选 NUMBER 字段以及判断表是否存在主键/唯一键/唯一索引
// 第一优先级配置文件指定字段【忽略是否存在索引】
// 第二优先级任意取某个主键/唯一索引 NUMBER 字段
// 第三优先级取某个唯一性 DISTINCT 高的索引 NUMBER 字段
// 如果表没有索引 NUMBER 字段或者没有 NUMBER 字段则报错
func (t *Task) FilterDBWhereColumn() (string, error) {
	// 以参数配置文件 indexFiledName 忽略是否存在索引，需要人工确认
	// 字段筛选优先级：配置文件优先级 > PK > UK > Index > Distinct Value

	// 获取表字段
	columnInfo, err := t.oracle.GetOracleSchemaTableColumn(t.cfg.OracleConfig.SchemaName, t.tableName, t.oracleCollation)
	if err != nil {
		return "", err
	}

	// number 数据类型字段
	var integerColumns []string
	for _, colsInfo := range columnInfo {
		// 数字
		if strings.EqualFold(strings.ToUpper(colsInfo["DATA_TYPE"]), "NUMBER") {
			integerColumns = append(integerColumns, colsInfo["COLUMN_NAME"])
		}
	}

	if len(integerColumns) == 0 {
		return "", fmt.Errorf("oracle schema [%s] table [%s] number column isn't exist, not support, pelase exclude skip or add number column index", t.cfg.OracleConfig.SchemaName, t.tableName)
	}

	// PK、UK
	var puConstraints []o2m.ConstraintPUKey
	pkInfo, err := t.oracle.GetOracleSchemaTablePrimaryKey(t.cfg.OracleConfig.SchemaName, t.tableName)
	if err != nil {
		return "", err
	}
	for _, pk := range pkInfo {
		puConstraints = append(puConstraints, o2m.ConstraintPUKey{
			ConstraintType:   "PK",
			ConstraintColumn: strings.ToUpper(pk["COLUMN_LIST"]),
		})
	}

	ukInfo, err := t.oracle.GetOracleSchemaTableUniqueKey(t.cfg.OracleConfig.SchemaName, t.tableName)
	if err != nil {
		return "", err
	}
	for _, pk := range ukInfo {
		puConstraints = append(puConstraints, o2m.ConstraintPUKey{
			ConstraintType:   "UK",
			ConstraintColumn: strings.ToUpper(pk["COLUMN_LIST"]),
		})
	}

	// 存放联合主键，联合唯一约束、联合唯一索引以及普通索引
	var indexArr []string

	if len(puConstraints) > 0 {
		for _, pu := range puConstraints {
			// 单列主键/唯一约束
			str := strings.Split(pu.ConstraintColumn, ",")
			if len(str) == 1 && common.IsContainString(integerColumns, strings.ToUpper(str[0])) {

				return strings.ToUpper(strings.Split(pu.ConstraintColumn, ",")[0]), nil
			}
			// 联合主键/唯一约束引导字段，跟普通索引 PK字段选择率
			indexArr = append(indexArr, pu.ConstraintColumn)
		}
	}

	// index
	var indexes []o2m.Index
	indexInfo, err := t.oracle.GetOracleSchemaTableNormalIndex(t.cfg.OracleConfig.SchemaName, t.tableName)
	if err != nil {
		return "", err
	}
	for _, indexCol := range indexInfo {
		indexes = append(indexes, o2m.Index{
			IndexInfo: o2m.IndexInfo{
				Uniqueness:  strings.ToUpper(indexCol["UNIQUENESS"]),
				IndexColumn: strings.ToUpper(indexCol["COLUMN_LIST"]),
			},
			IndexName:        strings.ToUpper(indexCol["INDEX_NAME"]),
			IndexType:        strings.ToUpper(indexCol["INDEX_TYPE"]),
			DomainIndexOwner: strings.ToUpper(indexCol["ITYP_OWNER"]),
			DomainIndexName:  strings.ToUpper(indexCol["ITYP_NAME"]),
			DomainParameters: strings.ToUpper(indexCol["PARAMETERS"]),
		})
	}

	indexInfo, err = t.oracle.GetOracleSchemaTableUniqueIndex(t.cfg.OracleConfig.SchemaName, t.tableName)
	if err != nil {
		return "", err
	}
	for _, indexCol := range indexInfo {
		indexes = append(indexes, o2m.Index{
			IndexInfo: o2m.IndexInfo{
				Uniqueness:  strings.ToUpper(indexCol["UNIQUENESS"]),
				IndexColumn: strings.ToUpper(indexCol["COLUMN_LIST"]),
			},
			IndexName:        strings.ToUpper(indexCol["INDEX_NAME"]),
			IndexType:        strings.ToUpper(indexCol["INDEX_TYPE"]),
			DomainIndexOwner: strings.ToUpper(indexCol["ITYP_OWNER"]),
			DomainIndexName:  strings.ToUpper(indexCol["ITYP_NAME"]),
			DomainParameters: strings.ToUpper(indexCol["PARAMETERS"]),
		})
	}

	var ukIndex, nonUkIndex []string
	if len(indexes) > 0 {
		for _, idx := range indexes {
			if idx.IndexType == "NORMAL" && idx.Uniqueness == "NONUNIQUE" {
				nonUkIndex = append(nonUkIndex, idx.IndexColumn)
			}
			if idx.IndexType == "NORMAL" && idx.Uniqueness == "UNIQUE" {
				ukIndex = append(ukIndex, idx.IndexColumn)
			}
		}
	}

	if len(ukIndex) > 0 {
		for _, uk := range ukIndex {
			// 单列唯一索引
			str := strings.Split(uk, ",")
			if len(str) == 1 && common.IsContainString(integerColumns, strings.ToUpper(str[0])) {

				return strings.ToUpper(str[0]), nil
			}
			// 联合唯一索引引导字段，跟普通索引 PK 字段选择率
			indexArr = append(indexArr, uk)
		}
	}

	// 如果表不存在主键/唯一键/唯一索引，直接返回报错中断，因为可能导致数据校验不准
	if len(puConstraints) == 0 && len(ukIndex) == 0 {
		return "", fmt.Errorf("oracle schema [%s] table [%s] pk/uk/unique index isn't exist, it's not support, please skip", t.cfg.OracleConfig.SchemaName, t.tableName)
	}

	// 普通索引、联合主键/联合唯一键/联合唯一索引，选择 number distinct 高的字段
	indexArr = append(indexArr, nonUkIndex...)

	orderCols, err := t.oracle.GetOracleTableColumnDistinctValue(t.cfg.OracleConfig.SchemaName, t.tableName, integerColumns)
	if err != nil {
		return "", fmt.Errorf("get oracle schema [%s] table [%s] column distinct values failed: %v", t.cfg.OracleConfig.SchemaName, t.tableName, err)
	}

	if len(indexArr) > 0 {
		for _, column := range orderCols {
			for _, index := range indexArr {
				if strings.EqualFold(column, strings.Split(index, ",")[0]) {
					return column, nil
				}
			}
		}
	}
	return "", fmt.Errorf("oracle schema [%s] table [%s] pk/uk/index number datatype column isn't exist, please skip or fixed", t.cfg.OracleConfig.SchemaName, t.tableName)
}

func (t *Task) IsPartitionTable() (string, error) {
	isOK, err := t.oracle.IsOraclePartitionTable(t.cfg.OracleConfig.SchemaName, t.tableName)
	if err != nil {
		return "", err
	}
	if isOK {
		return "YES", nil
	}
	return "NO", nil
}
