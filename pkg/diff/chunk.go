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
package diff

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/pkg/check"

	"github.com/wentaojin/transferdb/utils"

	"github.com/wentaojin/transferdb/service"
)

// Diff 数据对比
type Diff struct {
	ChunkSize                   int
	DiffThreads                 int
	OnlyCheckRows               bool
	IgnoreStructCheck           bool
	SourceSchema                string
	SourceTable                 string
	IndexFields                 string
	Range                       string
	SourceCharacterSet, NlsComp string
	SourceTableCollation        string
	SourceSchemaCollation       string
	OracleCollation             bool
	InsertBatchSize             int
	SyncMode                    string
	Engine                      *service.Engine `json:"-"`
}

func NewDiff(cfg *config.CfgFile, engine *service.Engine, exportTableSlice []string, sourceCharacterSet, nlsComp string,
	sourceTableCollation map[string]string,
	sourceSchemaCollation string,
	oracleCollation bool) ([]Diff, error) {
	// 单独表配置获取
	tableCFG := make(map[string]config.TableConfig)
	for _, tableCfg := range cfg.DiffConfig.TableConfig {
		tableCFG[strings.ToUpper(tableCfg.SourceTable)] = tableCfg
	}

	var diffTables []Diff
	for _, t := range exportTableSlice {
		d := Diff{
			ChunkSize:             cfg.DiffConfig.ChunkSize,
			DiffThreads:           cfg.DiffConfig.DiffThreads,
			OnlyCheckRows:         cfg.DiffConfig.OnlyCheckRows,
			IgnoreStructCheck:     cfg.DiffConfig.IgnoreStructCheck,
			SourceSchema:          strings.ToUpper(cfg.OracleConfig.SchemaName),
			SourceTable:           strings.ToUpper(t),
			Engine:                engine,
			SourceCharacterSet:    sourceCharacterSet,
			NlsComp:               nlsComp,
			SourceTableCollation:  sourceTableCollation[strings.ToUpper(t)],
			SourceSchemaCollation: sourceSchemaCollation,
			OracleCollation:       oracleCollation,
			InsertBatchSize:       cfg.AppConfig.InsertBatchSize,
			SyncMode:              utils.DiffMode,
		}
		if _, ok := tableCFG[strings.ToUpper(t)]; ok {
			d.IndexFields = tableCFG[strings.ToUpper(t)].IndexFields
			d.Range = tableCFG[strings.ToUpper(t)].Range
		}
		diffTables = append(diffTables, d)
	}
	return diffTables, nil
}

func (d *Diff) SplitChunk(workerID int, globalSCN uint64) error {
	numberCols, sourceColumnInfo, targetColumnInfo, err := AdjustTableSelectColumn(d.Engine, d.SourceSchema, d.SourceTable, d.SourceCharacterSet, d.NlsComp, d.SourceTableCollation, d.SourceSchemaCollation, d.OracleCollation, d.OnlyCheckRows)
	if err != nil {
		return err
	}

	numberColumn, err := FilterOracleNUMBERColumn(d.Engine, d.SourceSchema, d.SourceTable, d.IndexFields, numberCols, d.OnlyCheckRows)
	if err != nil {
		return err
	}

	// 参数优先级
	if d.OnlyCheckRows {
		// SELECT COUNT(1) FROM TAB WHERE 1=1
		if err = d.Engine.InitDataDiffMetaRecordByWhere(d.SourceSchema, d.SourceTable,
			"COUNT(1)", "COUNT(1)", "1 = 1", d.SyncMode, globalSCN); err != nil {
			return err
		}
		return nil
	}

	if !d.OnlyCheckRows && d.Range != "" {
		// TODO: Range 数据拆分
		if err = d.Engine.InitDataDiffMetaRecordByWhere(d.SourceSchema, d.SourceTable,
			sourceColumnInfo, targetColumnInfo, d.Range, d.SyncMode, globalSCN); err != nil {
			return err
		}
		return nil
	} else if !d.OnlyCheckRows && d.Range == "" {
		if err = d.Engine.InitDataDiffMetaRecordByNUMBER(d.SourceSchema, d.SourceTable,
			sourceColumnInfo, targetColumnInfo, numberColumn, globalSCN, workerID, d.ChunkSize, d.InsertBatchSize, d.SyncMode); err != nil {
			return err
		}
		return nil
	} else {
		return fmt.Errorf("config file parse failed, diff string [%v]", d.String())
	}
}

func (d *Diff) String() string {
	jsonByte, _ := json.Marshal(d)
	return string(jsonByte)
}

// 字段查询以 ORACLE 字段为主
// Date/Timestamp 字段类型格式化
// Interval Year/Day 数据字符 TO_CHAR 格式化
func AdjustTableSelectColumn(e *service.Engine, schemaName, tableName string, sourceCharacterSet, nlsComp string,
	sourceTableCollation string, sourceSchemaCollation string, oracleCollation bool, onlyCheckRows bool) ([]string, string, string, error) {
	var (
		sourceColumnInfo, targetColumnInfo []string
		numberCols                         []string
	)

	// 只对比数据行数
	if onlyCheckRows {
		return numberCols, "COUNT(1)", "COUNT(1)", nil
	}

	columns, _, err := check.GetOracleTableColumn(schemaName, tableName, e,
		strings.Split(sourceCharacterSet, ".")[1], nlsComp, sourceTableCollation, sourceSchemaCollation, oracleCollation)
	if err != nil {
		return []string{}, "", "", err
	}

	for colName, colsInfo := range columns {
		switch strings.ToUpper(colsInfo.DataType) {
		// 数字
		case "NUMBER":
			sourceColumnInfo = append(sourceColumnInfo, utils.StringsBuilder("DECODE(SUBSTR(", colName, ",1,1),'.','0' || ", colName, ",", colName, ") AS ", colName))
			targetColumnInfo = append(targetColumnInfo, utils.StringsBuilder("CAST(0 + CAST(", colName, " AS CHAR) AS CHAR) AS ", colName))
			numberCols = append(numberCols, colName)
		case "DECIMAL", "DEC", "DOUBLE PRECISION", "FLOAT", "INTEGER", "INT", "REAL", "NUMERIC", "BINARY_FLOAT", "BINARY_DOUBLE", "SMALLINT":
			sourceColumnInfo = append(sourceColumnInfo, utils.StringsBuilder("DECODE(SUBSTR(", colName, ",1,1),'.','0' || ", colName, ",", colName, ") AS ", colName))
			targetColumnInfo = append(targetColumnInfo, utils.StringsBuilder("CAST(0 + CAST(", colName, " AS CHAR) AS CHAR) AS ", colName))
		// 字符
		case "BFILE", "CHARACTER", "LONG", "NCHAR VARYING", "ROWID", "UROWID", "VARCHAR", "XMLTYPE", "CHAR", "NCHAR", "NVARCHAR2", "NCLOB", "CLOB":
			sourceColumnInfo = append(sourceColumnInfo, utils.StringsBuilder("NVL(", colName, ",'') AS ", colName))
			targetColumnInfo = append(targetColumnInfo, utils.StringsBuilder("IFNULL(", colName, ",'') AS ", colName))
		// 二进制
		case "BLOB", "LONG RAW", "RAW":
			sourceColumnInfo = append(sourceColumnInfo, colName)
			targetColumnInfo = append(targetColumnInfo, colName)
		// 时间
		case "DATE":
			sourceColumnInfo = append(sourceColumnInfo, utils.StringsBuilder("TO_CHAR(", colName, ",'yyyy-MM-dd HH24:mi:ss') AS ", colName))
			targetColumnInfo = append(targetColumnInfo, utils.StringsBuilder("DATE_FORMAT(", colName, ",'%Y-%m-%d %H:%i:%s') AS ", colName))
		// 默认其他类型
		default:
			if strings.Contains(colsInfo.DataType, "INTERVAL") {
				sourceColumnInfo = append(sourceColumnInfo, utils.StringsBuilder("TO_CHAR(", colName, ") AS ", colName))
				targetColumnInfo = append(targetColumnInfo, colName)
			} else if strings.Contains(colsInfo.DataType, "TIMESTAMP") {
				sourceColumnInfo = append(sourceColumnInfo, utils.StringsBuilder("TO_CHAR(", colName, ",'yyyy-MM-dd HH24:mi:ss') AS ", colName))
				targetColumnInfo = append(targetColumnInfo, utils.StringsBuilder("FROM_UNIXTIME(UNIX_TIMESTAMP(", colName, "),'%Y-%m-%d %H:%i:%s') AS ", colName))
			} else {
				sourceColumnInfo = append(sourceColumnInfo, colName)
				targetColumnInfo = append(targetColumnInfo, colName)
			}
		}
	}

	return numberCols, strings.Join(sourceColumnInfo, ","), strings.Join(targetColumnInfo, ","), nil
}

// 筛选 NUMBER 字段以及判断表是否存在主键/唯一键/唯一索引
// 第一优先级配置文件指定字段【忽略是否存在索引】
// 第二优先级任意取某个主键/唯一索引 NUMBER 字段
// 第三优先级取某个唯一性 DISTINCT 高的索引 NUMBER 字段
// 如果表没有索引 NUMBER 字段或者没有 NUMBER 字段则报错
func FilterOracleNUMBERColumn(e *service.Engine, schemaName, tableName, indexFiledName string, numberCols []string, onlyCheckRows bool) (string, error) {
	// 只对比数据行，忽略 numberCol 字段
	// SELECT COUNT(1) FROM TAB WHERE 1=1
	if onlyCheckRows {
		return "", nil
	}
	// 以参数配置文件 indexFiledName 为准，忽略是否存在索引
	if indexFiledName != "" {
		isNUMBER, err := e.IsNumberColumnTYPE(schemaName, tableName, indexFiledName)
		if err != nil || !isNUMBER {
			return "", err
		}
		return indexFiledName, nil
	}

	if len(numberCols) == 0 {
		return "", fmt.Errorf("oracle schema [%s] table [%s] number column isn't exist, not support, pelase exclude skip or add number column index", schemaName, tableName)
	}

	puKeys, _, _, err := check.GetOracleConstraint(schemaName, tableName, e)
	if err != nil {
		return "", err
	}

	// 存放联合主键，联合唯一约束、联合唯一索引以及普通索引
	var indexArr []string

	if len(puKeys) > 0 {
		for _, pu := range puKeys {
			// 单列主键/唯一约束
			// 联合主键/唯一约束引导字段，跟普通索引 PK字段选择率
			str := strings.Split(pu.ConstraintColumn, ",")
			if len(str) == 1 && utils.IsContainString(numberCols, strings.ToUpper(str[0])) {
				return strings.ToUpper(strings.Split(pu.ConstraintColumn, ",")[0]), nil
			}
			indexArr = append(indexArr, pu.ConstraintColumn)
		}
	}

	indexes, err := check.GetOracleTableIndex(schemaName, tableName, e)
	if err != nil {
		return "", err
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
			// 联合唯一索引引导字段，跟普通索引 PK 字段选择率
			str := strings.Split(uk, ",")
			if len(str) == 1 && utils.IsContainString(numberCols, strings.ToUpper(str[0])) {
				return strings.ToUpper(str[0]), nil
			}
			indexArr = append(indexArr, uk)
		}
	}

	// 如果表不存在主键/唯一键/唯一索引，直接返回报错中断，因为可能导致数据校验不准
	if len(puKeys) == 0 && len(ukIndex) == 0 {
		return "", fmt.Errorf("oracle schema [%s] table [%s] pk/uk/unique index isn't exist, it's not support, please skip", schemaName, tableName)
	}

	// 普通索引、联合主键/联合唯一键/联合唯一索引，选择 number distinct 高的字段
	indexArr = append(indexArr, nonUkIndex...)

	orderCols, err := e.GetOracleTableColumnDistinctValue(schemaName, tableName, numberCols)
	if err != nil {
		return "", fmt.Errorf("get oracle schema [%s] table [%s] column distinct values failed: %v", schemaName, tableName, err)
	}

	if len(indexArr) > 0 {
		for _, c := range orderCols {
			for _, index := range indexArr {
				if strings.EqualFold(c, strings.Split(index, ",")[0]) {
					return c, nil
				}
			}
		}
	}
	return "", fmt.Errorf("oracle schema [%s] table [%s] pk/uk/index number datatype column isn't exist, please skip or fixed", schemaName, tableName)
}
