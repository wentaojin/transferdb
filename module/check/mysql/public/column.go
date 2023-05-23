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
package public

import (
	"encoding/json"
	"github.com/wentaojin/transferdb/common"
)

type Table struct {
	SchemaName         string
	TableName          string
	TableComment       string
	TableCharacterSet  string
	TableCollation     string
	Columns            map[string]Column // KEY 字段名
	Indexes            []Index
	PUConstraints      []ConstraintPUKey
	ForeignConstraints []ConstraintForeign
	CheckConstraints   []ConstraintCheck
	IsPartition        bool
	Partitions         []Partition // 获取分区键、分区类型以及子分区键、子分区类型
}

type Column struct {
	DataType                string
	CharLength              string
	CharUsed                string
	CharacterSet            string
	Collation               string
	OracleOriginDataDefault string
	MySQLOriginDataDefault  string
	ColumnInfo
}

type ColumnInfo struct {
	DataLength        string
	DataPrecision     string
	DataScale         string
	DatetimePrecision string
	NULLABLE          string
	DataDefault       string
	Comment           string
}

type Index struct {
	IndexInfo
	IndexName        string
	IndexType        string
	DomainIndexOwner string
	DomainIndexName  string
	DomainParameters string
}

type IndexInfo struct {
	Uniqueness  string
	IndexColumn string
}

type ConstraintPUKey struct {
	ConstraintType   string
	ConstraintColumn string
}

type ConstraintForeign struct {
	ColumnName            string
	ReferencedTableSchema string
	ReferencedTableName   string
	ReferencedColumnName  string
	DeleteRule            string
	UpdateRule            string
}

type ConstraintCheck struct {
	ConstraintExpression string
}

type Partition struct {
	PartitionKey     string
	PartitionType    string
	SubPartitionKey  string
	SubPartitionType string
}

func (t *Table) String(jsonType string) string {
	var jsonStr []byte
	switch jsonType {
	case common.JSONColumns:
		jsonStr, _ = json.Marshal(t.Columns)
	case common.JSONPUConstraint:
		jsonStr, _ = json.Marshal(t.PUConstraints)
	case common.JSONFKConstraint:
		jsonStr, _ = json.Marshal(t.ForeignConstraints)
	case common.JSONCKConstraint:
		jsonStr, _ = json.Marshal(t.CheckConstraints)
	case common.JSONIndex:
		jsonStr, _ = json.Marshal(t.Indexes)
	case common.JSONPartition:
		jsonStr, _ = json.Marshal(t.Partitions)
	}
	return string(jsonStr)
}

func (c *Column) String() string {
	jsonStr, _ := json.Marshal(c)
	return string(jsonStr)
}
