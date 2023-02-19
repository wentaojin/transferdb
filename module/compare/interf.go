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
package compare

type Processor interface {
	AdjustDBSelectColumn() (sourceColumnInfo string, targetColumnInfo string, err error)
	FilterDBWhereColumn() (string, error)
	IsPartitionTable() (string, error)
}

type Chunker interface {
	CustomTableConfig() (customColumn string, customRange string, err error)
	Split() error
}

type Reporter interface {
	GenDBQuery() (oracleQuery string, mysqlQuery string)
	CheckOracleRows(oracleQuery string) (int64, error)
	CheckMySQLRows(mysqlQuery string) (int64, error)
	ReportCheckRows() (string, error)
	ReportCheckCRC32() (string, error)
	Report() (string, error)
}

type Comparer interface {
	NewCompare() error
}
