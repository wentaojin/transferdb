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
package cost

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/text"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/wentaojin/transferdb/service"
)

/*
	Oracle Database Overview
*/
func GatherOracleDBOverview(engine *service.Engine) (string, error) {
	dbName, platformID, platformName, err := engine.GetOracleDBName()
	if err != nil {
		return "", err
	}

	globalName, err := engine.GetOracleGlobalName()
	if err != nil {
		return "", err
	}
	dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err := engine.GetOracleParameters()
	if err != nil {
		return "", err
	}

	instanceRes, err := engine.GetOracleInstance()
	if err != nil {
		return "", err
	}

	dataSize, err := engine.GetOracleDataTotal()
	if err != nil {
		return "", err
	}

	hostCPU, err := engine.GetOracleNumCPU()
	if err != nil {
		return "", err
	}
	memorySize, err := engine.GetOracleMemoryGB()
	if err != nil {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Database Overview", "Database Overview"}, table.RowConfig{AutoMerge: true})

	t.AppendRows([]table.Row{
		{"Host Name", instanceRes[0]["HOST_NAME"]},
		{"Platform Name/ID", fmt.Sprintf("%s/%s", platformName, platformID)},
		{"Database Name", dbName},
		{"Global Database Name", globalName},
		{"Clustered Database", clusterDatabase},
		{"Clustered Database Instances", CLusterDatabaseInstance},
		{"Instance Name", instanceRes[0]["INSTANCE_NAME"]},
		{"Instance Number", instanceRes[0]["INSTANCE_NUMBER"]},
		{"Thread Number", instanceRes[0]["THREAD_NUMBER"]},
		{"Database Block Size(KB)", dbBlockSize},
		{"Database Total Used Size(GB)", dataSize},
		{"Host Cpus(single)", hostCPU},
		{"Host Mem(GB)", memorySize},
		{"Character Set", characterSet},
	})

	t.SetCaption("The database overview.\n")

	return t.Render(), nil
}

func GatherOracleSchemaOverview(schemaName []string, engine *service.Engine) (string, error) {
	overview, err := engine.GetOracleSchemaOverview(schemaName)
	if err != nil {
		return "", err
	}

	if len(overview) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Schema Overview", "Schema Overview", "Schema Overview", "Schema Overview", "Schema Overview", "Schema Overview"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Size/GB", "Index Size/GB", "Lob Table Size/GB", "Lob Index Size/GB", "ALL Tables ROWS"})

	var tableRows []table.Row
	for _, ow := range overview {
		tableRows = append(tableRows, table.Row{
			ow["SCHEMA"], ow["TABLE"], ow["INDEX"], ow["LOBTABLE"], ow["LOBINDEX"], ow["ROWCOUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetCaption("The database schema table/partition/subpartition size overview.\n")

	return t.Render(), nil
}

func GatherOracleSchemaTableRowsTOP(schemaName []string, engine *service.Engine) (string, error) {
	overview, err := engine.GetOracleSchemaTableRowsTOP(schemaName)
	if err != nil {
		return "", nil
	}
	if len(overview) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table TOP 10", "Table TOP 10", "Table TOP 10", "Table TOP 10"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table NAME", "Table Type", "Table Size/GB"})

	var tableRows []table.Row
	for _, ow := range overview {
		tableRows = append(tableRows, table.Row{
			ow["SCHEMA"], ow["SEGMENT_NAME"], ow["SEGMENT_TYPE"], ow["TABLE_SIZE"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The schema table data size top 10 overview.\n")

	return t.Render(), nil
}

func GatherOracleSchemaObjectOverview(schemaName []string, engine *service.Engine) (string, error) {
	overview, err := engine.GetOracleObjectTypeOverview(schemaName)
	if err != nil {
		return "", nil
	}

	if len(overview) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Object Overview", "Object Overview", "Object Overview"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Object_Type", "Counts"})

	var tableRows []table.Row
	for _, ow := range overview {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["OBJECT_TYPE"], ow["COUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema object type overview.\n")

	return t.Render(), nil

}

func GatherOracleSchemaPartitionType(schemaName []string, engine *service.Engine) (string, error) {
	partitionInfo, err := engine.GetOraclePartitionObjectType(schemaName)
	if err != nil {
		return "", err
	}

	if len(partitionInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Partition Type", "Partition Type", "Partition Type", "Partition Type"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Partition Type", "SubPartition Type"})

	var tableRows []table.Row
	for _, ow := range partitionInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["TABLE_NAME"], ow["PARTITIONING_TYPE"], ow["SUBPARTITIONING_TYPE"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema partition type overview.\n")

	return t.Render(), nil
}

func GatherOracleSchemaColumnTypeAndMaxLength(schemaName []string, engine *service.Engine) (string, error) {
	columnInfo, err := engine.GetOracleColumnTypeAndMaxLength(schemaName)
	if err != nil {
		return "", err
	}

	if len(columnInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Column Object Type", "Column Object Type", "Column Object Type", "Column Object Type"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Data Type", "Counts", "Max Data Length"})

	var tableRows []table.Row
	for _, ow := range columnInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["DATA_TYPE"], ow["COUNT"], ow["MAX_DATA_LENGTH"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema column type and max data length overview.\n")

	return t.Render(), nil
}

func GatherOracleSchemaTableAvgRowLength(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleAvgRowLength(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Avg Row Length/By Statistics", "Avg Row Length/By Statistics", "Avg Row Length/By Statistics"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Avg Row Length/Bytes"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["TABLE_NAME"], ow["AVG_ROW_LEN"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema table avg row length overview.\n")

	return t.Render(), nil
}

func GatherOracleSchemaTemporaryTable(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTemporaryTable(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Temporary Table", "Temporary Table"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "COUNT"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["COUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema temporary table counts overview.\n")

	return t.Render(), nil
}

/*
	Oracle Database Type
*/
func GatherOracleSchemaIndexType(schemaName []string, engine *service.Engine) (string, error) {
	columnInfo, err := engine.GetOracleIndexType(schemaName)
	if err != nil {
		return "", err
	}

	if len(columnInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Index Object Type", "Index Object Type", "Index Object Type"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Index Type", "Counts"})

	var tableRows []table.Row
	for _, ow := range columnInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["INDEX_TYPE"], ow["COUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema index type and counts overview.\n")

	return t.Render(), nil
}

func GatherOracleConstraintType(schemaName []string, engine *service.Engine) (string, error) {
	columnInfo, err := engine.GetOracleConstraintType(schemaName)
	if err != nil {
		return "", err
	}

	if len(columnInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Constraint Object Type", "Constraint Object Type", "Constraint Object Type"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Constraint Type", "Counts"})

	var tableRows []table.Row
	for _, ow := range columnInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["CONSTRAINT_TYPE"], ow["COUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema constraint type and counts overview.\n")

	return t.Render(), nil
}

func GatherOracleSchemeCodeType(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleCodeObject(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Code Object Type", "Code Object Type", "Code Object Type", "Code Object Type"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "NAME", "TYPE", "Code Total LINES"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["NAME"], ow["TYPE"], ow["LINES"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema code type and total lines overview.\n")

	return t.Render(), nil
}

func GatherOracleSchemaSynonymType(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleSynonymObjectType(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Synonym Object Type", "Synonym Object Type"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "COunt"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["COUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema synonym type and counts overview.\n")

	return t.Render(), nil
}

/*
	Oracle Database Check
*/
func GatherOraclePartitionTableCountsCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOraclePartitionTableOver1024(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Partition Table Over 1024", "Partition Table Over 1024", "Partition Table Over 1024"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Partition Counts"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["TABLE_NAME"], ow["PARTITION_COUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema partition table counts overview.\n")

	return t.Render(), nil
}

func GatherOracleTableRowLengthCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableRowLengthOver6M(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table Row Length Over 6M", "Table Row Length Over 6M", "Table Row Length Over 6M"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Avg Row Length/Bytes"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["TABLE_NAME"], ow["AVG_ROW_LEN"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema table avg row length over 6M overview.\n")

	return t.Render(), nil
}

func GatherOracleTableIndexRowLengthCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableIndexLengthOver3072(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Index Column Length Over 3072", "Index Column Length Over 3072", "Index Column Length Over 3072", "Index Column Length Over 3072"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Index Name", "Column Length"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["INDEX_OWNER"], ow["TABLE_NAME"], ow["INDEX_NAME"], ow["COUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema table index column length over 3072 overview.\n")

	return t.Render(), nil
}

func GatherOracleTableColumnCountsCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableColumnCountsOver512(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table Column Counts Over 512", "Table Column Counts Over 512", "Table Column Counts Over 512"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Counts"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["TABLE_NAME"], ow["COUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema table column counts over 512 overview.\n")

	return t.Render(), nil
}

func GatherOracleTableIndexCountsCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableIndexCountsOver64(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table Index Counts Over 64", "Table Index Counts Over 64", "Table Index Counts Over 64"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Counts"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["TABLE_OWNER"], ow["TABLE_NAME"], ow["COUNT"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})

	t.SetCaption("The database schema table index counts over 64 overview.\n")

	return t.Render(), nil
}

func GatherOracleTableNumberTypeCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableNumberTypeCheck(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table Number Type Check", "Table Number Type Check", "Table Number Type Check", "Table Number Type Check", "Table Number Type Check"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Column Name", "Data Precision", "Data Scale"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["TABLE_NAME"], ow["COLUMN_NAME"], ow["DATA_PRECISION"], ow["DATA_SCALE"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})
	t.SetCaption("The database number type has no scale check, and the Oracle non-scale number type can store integer and floating-point numbers at the same time. The specific type of storage needs to be confirmed with the development. Stored integers can be converted to bigint, and stored floating-point numbers can be converted to the corresponding decimal type.\n")

	return t.Render(), nil
}

func GatherOracleUsernameLengthCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleUsernameLength(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Username Length Check", "Username Length Check", "Username Length Check"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "ACCOUNT_STATUS", "CREATED"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["USERNAME"], ow["ACCOUNT_STATUS"], ow["CREATED"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})
	t.SetCaption("The length of the database user name is greater than 64.\n")

	return t.Render(), nil
}

func GatherOracleTableNameLengthCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableNameLength(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table Name Length Check", "Table Name Length Check"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["TABLE_NAME"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})
	t.SetCaption("The length of the database table name is greater than 64.\n")

	return t.Render(), nil
}

func GatherOracleColumnNameLengthCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableColumnNameLength(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}

	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table Column Name Length Check", "Table Column Name Length Check"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Column Name"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["TABLE_NAME"], ow["COLUMN_NAME"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})
	t.SetCaption("The length of the database table column name is greater than 64.\n")

	return t.Render(), nil
}

func GatherOracleIndexNameLengthCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableIndexNameLength(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table Index Name Length Check", "Table Index Name Length Check", "Table Index Name Length Check"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Table Name", "Index Name"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["INDEX_OWNER"], ow["TABLE_NAME"], ow["INDEX_NAME"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})
	t.SetCaption("The length of the database table index name is greater than 64.\n")

	return t.Render(), nil
}

func GatherOracleViewNameLengthCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableViewNameLength(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table View Name Length Check", "Table View Name Length Check", "Table View Name Length Check"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "View Name", "Read Only"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["OWNER"], ow["VIEW_NAME"], ow["READ_ONLY"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})
	t.SetCaption("The length of the database table view name is greater than 64.\n")

	return t.Render(), nil
}

func GatherOracleSequenceNameLengthCheck(schemaName []string, engine *service.Engine) (string, error) {
	synonymInfo, err := engine.GetOracleTableSequenceNameLength(schemaName)
	if err != nil {
		return "", err
	}

	if len(synonymInfo) == 0 {
		return "", err
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table Sequence Name Length Check", "Table Sequence Name Length Check", "Table Sequence Name Length Check"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "Sequence Name", "Order Flag"})

	var tableRows []table.Row
	for _, ow := range synonymInfo {
		tableRows = append(tableRows, table.Row{
			ow["SEQUENCE_OWNER"], ow["SEQUENCE_NAME"], ow["ORDER_FLAG"],
		})
	}
	t.AppendRows(tableRows)

	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, AutoMerge: true},
	})
	t.SetCaption("The length of the database table sequence name is greater than 64.\n")

	return t.Render(), nil
}
