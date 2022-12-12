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
	"fmt"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/wentaojin/transferdb/database/oracle"

	"github.com/jedib0t/go-pretty/v6/table"
)

/*
Oracle Database Overview
*/
func GatherOracleDBOverview(oracle *oracle.Oracle, reportName, reportUser string) (*ReportOverview, error) {
	dbName, platformID, platformName, err := oracle.GetOracleDBName()
	if err != nil {
		return &ReportOverview{}, err
	}

	globalName, err := oracle.GetOracleGlobalName()
	if err != nil {
		return &ReportOverview{}, err
	}
	dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err := oracle.GetOracleParameters()
	if err != nil {
		return &ReportOverview{}, err
	}

	instanceRes, err := oracle.GetOracleInstance()
	if err != nil {
		return &ReportOverview{}, err
	}

	dataSize, err := oracle.GetOracleDataTotal()
	if err != nil {
		return &ReportOverview{}, err
	}

	hostCPU, err := oracle.GetOracleNumCPU()
	if err != nil {
		return &ReportOverview{}, err
	}
	memorySize, err := oracle.GetOracleMemoryGB()
	if err != nil {
		return &ReportOverview{}, err
	}

	return &ReportOverview{
		ReportName:        reportName,
		ReportUser:        reportUser,
		HostName:          instanceRes[0]["HOST_NAME"],
		PlatformName:      fmt.Sprintf("%s/%s", platformName, platformID),
		DBName:            dbName,
		GlobalDBName:      globalName,
		ClusterDB:         clusterDatabase,
		ClusterDBInstance: CLusterDatabaseInstance,
		InstanceName:      instanceRes[0]["INSTANCE_NAME"],
		InstanceNumber:    instanceRes[0]["INSTANCE_NUMBER"],
		ThreadNumber:      instanceRes[0]["THREAD_NUMBER"],
		BlockSize:         dbBlockSize,
		TotalUsedSize:     dataSize,
		HostCPUS:          hostCPU,
		HostMem:           memorySize,
		CharacterSet:      characterSet}, nil
}

func GatherOracleSchemaOverview(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableSizeData, error) {
	overview, err := oracle.GetOracleSchemaOverview(schemaName)
	if err != nil {
		return []ListSchemaTableSizeData{}, err
	}

	if len(overview) == 0 {
		return []ListSchemaTableSizeData{}, nil
	}

	var listData []ListSchemaTableSizeData
	for _, ow := range overview {
		listData = append(listData, ListSchemaTableSizeData{
			Schema:        ow["SCHEMA"],
			TableSize:     ow["TABLE"],
			IndexSize:     ow["INDEX"],
			LobTableSize:  ow["LOBTABLE"],
			LobIndexSize:  ow["LOBINDEX"],
			AllTablesRows: ow["ROWCOUNT"],
		})
	}

	return listData, nil
}

func GatherOracleMaxActiveSessionCount(oracle *oracle.Oracle) ([]ListSchemaActiveSession, error) {
	listActiveSession, err := oracle.GetOracleMaxActiveSessionCount()
	if err != nil {
		return []ListSchemaActiveSession{}, err
	}

	if len(listActiveSession) == 0 {
		return []ListSchemaActiveSession{}, nil
	}

	var listData []ListSchemaActiveSession
	for _, ow := range listActiveSession {
		listData = append(listData, ListSchemaActiveSession{
			Rownum:         ow["ROWNUM"],
			DBID:           ow["DBID"],
			InstanceNumber: ow["INSTANCE_NUMBER"],
			SampleID:       ow["SAMPLE_ID"],
			SampleTime:     ow["SAMPLE_TIME"],
			SessionCounts:  ow["SESSION_COUNT"],
		})
	}

	return listData, nil
}

func GatherOracleSchemaTableRowsTOP(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableRowsTOP, error) {
	overview, err := oracle.GetOracleSchemaTableRowsTOP(schemaName)
	if err != nil {
		return []ListSchemaTableRowsTOP{}, err
	}
	if len(overview) == 0 {
		return []ListSchemaTableRowsTOP{}, nil
	}

	var listData []ListSchemaTableRowsTOP
	for _, ow := range overview {
		listData = append(listData, ListSchemaTableRowsTOP{
			Schema:    ow["SCHEMA"],
			TableName: ow["SEGMENT_NAME"],
			TableType: ow["SEGMENT_TYPE"],
			TableSize: ow["TABLE_SIZE"],
		})
	}

	return listData, nil
}

func GatherOracleSchemaObjectOverview(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableObjectCounts, error) {
	overview, err := oracle.GetOracleObjectTypeOverview(schemaName)
	if err != nil {
		return []ListSchemaTableObjectCounts{}, err
	}

	if len(overview) == 0 {
		return []ListSchemaTableObjectCounts{}, nil
	}

	var listData []ListSchemaTableObjectCounts
	for _, ow := range overview {
		listData = append(listData, ListSchemaTableObjectCounts{
			Schema:     ow["OWNER"],
			ObjectType: ow["OBJECT_TYPE"],
			Counts:     ow["COUNT"],
		})

	}

	return listData, nil

}

func GatherOracleSchemaPartitionType(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTablePartitionType, error) {
	partitionInfo, err := oracle.GetOraclePartitionObjectType(schemaName)
	if err != nil {
		return []ListSchemaTablePartitionType{}, err
	}

	if len(partitionInfo) == 0 {
		return []ListSchemaTablePartitionType{}, nil
	}

	var listData []ListSchemaTablePartitionType
	for _, ow := range partitionInfo {
		listData = append(listData, ListSchemaTablePartitionType{
			Schema:           ow["OWNER"],
			TableName:        ow["TABLE_NAME"],
			PartitionType:    ow["PARTITIONING_TYPE"],
			SubPartitionType: ow["SUBPARTITIONING_TYPE"],
		})

	}

	return listData, nil
}

func GatherOracleSchemaColumnTypeAndMaxLength(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableColumnTypeAndMaxLength, error) {
	columnInfo, err := oracle.GetOracleColumnTypeAndMaxLength(schemaName)
	if err != nil {
		return []ListSchemaTableColumnTypeAndMaxLength{}, err
	}

	if len(columnInfo) == 0 {
		return []ListSchemaTableColumnTypeAndMaxLength{}, nil
	}

	var listData []ListSchemaTableColumnTypeAndMaxLength
	for _, ow := range columnInfo {
		listData = append(listData, ListSchemaTableColumnTypeAndMaxLength{
			Schema:        ow["OWNER"],
			DataType:      ow["DATA_TYPE"],
			Counts:        ow["COUNT"],
			MaxDataLength: ow["MAX_DATA_LENGTH"],
		})
	}

	return listData, nil
}

func GatherOracleSchemaTableAvgRowLength(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableAvgRowLength, error) {
	synonymInfo, err := oracle.GetOracleAvgRowLength(schemaName)
	if err != nil {
		return []ListSchemaTableAvgRowLength{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableAvgRowLength{}, nil
	}

	var listData []ListSchemaTableAvgRowLength
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTableAvgRowLength{
			Schema:       ow["OWNER"],
			TableName:    ow["TABLE_NAME"],
			AvgRowLength: ow["AVG_ROW_LEN"],
		})
	}

	return listData, nil
}

func GatherOracleSchemaTemporaryTable(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTemporaryTableCounts, error) {
	synonymInfo, err := oracle.GetOracleTemporaryTable(schemaName)
	if err != nil {
		return []ListSchemaTemporaryTableCounts{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTemporaryTableCounts{}, nil
	}

	var listData []ListSchemaTemporaryTableCounts
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTemporaryTableCounts{
			Schema: ow["OWNER"],
			Counts: ow["COUNT"],
		})
	}

	return listData, nil
}

/*
Oracle Database Type
*/
func GatherOracleSchemaIndexType(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaIndexType, error) {
	columnInfo, err := oracle.GetOracleIndexType(schemaName)
	if err != nil {
		return []ListSchemaIndexType{}, err
	}

	if len(columnInfo) == 0 {
		return []ListSchemaIndexType{}, nil
	}

	var listData []ListSchemaIndexType
	for _, ow := range columnInfo {
		listData = append(listData, ListSchemaIndexType{
			Schema:    ow["OWNER"],
			IndexType: ow["INDEX_TYPE"],
			Counts:    ow["COUNT"],
		})
	}

	return listData, nil
}

func GatherOracleConstraintType(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaConstraintType, error) {
	columnInfo, err := oracle.GetOracleConstraintType(schemaName)
	if err != nil {
		return []ListSchemaConstraintType{}, err
	}

	if len(columnInfo) == 0 {
		return []ListSchemaConstraintType{}, nil
	}

	var listData []ListSchemaConstraintType
	for _, ow := range columnInfo {
		listData = append(listData, ListSchemaConstraintType{
			Schema:         ow["OWNER"],
			ConstraintType: ow["CONSTRAINT_TYPE"],
			Counts:         ow["COUNT"],
		})
	}

	return listData, nil
}

func GatherOracleSchemeCodeType(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaCodeType, error) {
	synonymInfo, err := oracle.GetOracleCodeObject(schemaName)
	if err != nil {
		return []ListSchemaCodeType{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaCodeType{}, nil
	}

	var listData []ListSchemaCodeType
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaCodeType{
			Schema:   ow["OWNER"],
			CodeName: ow["NAME"],
			CodeType: ow["TYPE"],
			Counts:   ow["LINES"],
		})
	}

	return listData, nil
}

func GatherOracleSchemaSynonymType(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaSynonymType, error) {
	synonymInfo, err := oracle.GetOracleSynonymObjectType(schemaName)
	if err != nil {
		return []ListSchemaSynonymType{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaSynonymType{}, nil
	}

	var listData []ListSchemaSynonymType
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaSynonymType{
			Schema:     ow["OWNER"],
			TableOwner: ow["TABLE_OWNER"],
			Counts:     ow["COUNT"],
		})

	}

	return listData, nil
}

/*
Oracle Database Check
*/
func GatherOraclePartitionTableCountsCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaPartitionTableCountsCheck, error) {
	synonymInfo, err := oracle.GetOraclePartitionTableOver1024(schemaName)
	if err != nil {
		return []ListSchemaPartitionTableCountsCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaPartitionTableCountsCheck{}, nil
	}

	var listData []ListSchemaPartitionTableCountsCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaPartitionTableCountsCheck{
			Schema:          ow["OWNER"],
			TableName:       ow["TABLE_NAME"],
			PartitionCounts: ow["PARTITION_COUNT"],
		})
	}

	return listData, nil
}

func GatherOracleTableRowLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableRowLengthCheck, error) {
	synonymInfo, err := oracle.GetOracleTableRowLengthOver6M(schemaName)
	if err != nil {
		return []ListSchemaTableRowLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableRowLengthCheck{}, nil
	}

	var listData []ListSchemaTableRowLengthCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTableRowLengthCheck{
			Schema:       ow["OWNER"],
			TableName:    ow["TABLE_NAME"],
			AvgRowLength: ow["AVG_ROW_LEN"],
		})
	}

	return listData, nil
}

func GatherOracleTableIndexRowLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableIndexRowLengthCheck, error) {
	synonymInfo, err := oracle.GetOracleTableIndexLengthOver3072(schemaName)
	if err != nil {
		return []ListSchemaTableIndexRowLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableIndexRowLengthCheck{}, nil
	}

	var listData []ListSchemaTableIndexRowLengthCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTableIndexRowLengthCheck{
			Schema:       ow["INDEX_OWNER"],
			TableName:    ow["TABLE_NAME"],
			IndexName:    ow["INDEX_NAME"],
			ColumnLength: ow["COUNT"],
		})
	}

	return listData, nil
}

func GatherOracleTableColumnCountsCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableAndIndexCountsCheck, error) {
	synonymInfo, err := oracle.GetOracleTableColumnCountsOver512(schemaName)
	if err != nil {
		return []ListSchemaTableAndIndexCountsCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableAndIndexCountsCheck{}, nil
	}

	var listData []ListSchemaTableAndIndexCountsCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTableAndIndexCountsCheck{
			Schema:    ow["OWNER"],
			TableName: ow["TABLE_NAME"],
			Counts:    ow["COUNT"],
		})
	}

	return listData, nil
}

func GatherOracleTableIndexCountsCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableAndIndexCountsCheck, error) {
	synonymInfo, err := oracle.GetOracleTableIndexCountsOver64(schemaName)
	if err != nil {
		return []ListSchemaTableAndIndexCountsCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableAndIndexCountsCheck{}, nil
	}

	var listData []ListSchemaTableAndIndexCountsCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTableAndIndexCountsCheck{
			Schema:    ow["TABLE_OWNER"],
			TableName: ow["TABLE_NAME"],
			Counts:    ow["COUNT"],
		})
	}

	return listData, nil
}

func GatherOracleTableNumberTypeCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableNumberTypeCheck, error) {
	synonymInfo, err := oracle.GetOracleTableNumberTypeCheck(schemaName)
	if err != nil {
		return []ListSchemaTableNumberTypeCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableNumberTypeCheck{}, nil
	}

	var listData []ListSchemaTableNumberTypeCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTableNumberTypeCheck{
			Schema:        ow["OWNER"],
			TableName:     ow["TABLE_NAME"],
			ColumnName:    ow["COLUMN_NAME"],
			DataPrecision: ow["DATA_PRECISION"],
			DataScale:     ow["DATA_SCALE"],
		})
	}

	return listData, nil
}

func GatherOracleUsernameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]ListUsernameLengthCheck, error) {
	synonymInfo, err := oracle.GetOracleUsernameLength(schemaName)
	if err != nil {
		return []ListUsernameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListUsernameLengthCheck{}, nil
	}

	var listData []ListUsernameLengthCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListUsernameLengthCheck{
			Schema:        ow["USERNAME"],
			AccountStatus: ow["ACCOUNT_STATUS"],
			Created:       ow["CREATED"],
		})
	}

	return listData, nil
}

func GatherOracleTableNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableNameLengthCheck, error) {
	synonymInfo, err := oracle.GetOracleTableNameLength(schemaName)
	if err != nil {
		return []ListSchemaTableNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableNameLengthCheck{}, nil
	}

	var listData []ListSchemaTableNameLengthCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTableNameLengthCheck{
			Schema:    ow["OWNER"],
			TableName: ow["TABLE_NAME"],
		})
	}

	return listData, nil
}

func GatherOracleColumnNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableColumnNameLengthCheck, error) {
	synonymInfo, err := oracle.GetOracleTableColumnNameLength(schemaName)
	if err != nil {
		return []ListSchemaTableColumnNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableColumnNameLengthCheck{}, nil
	}

	var listData []ListSchemaTableColumnNameLengthCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTableColumnNameLengthCheck{
			Schema:     ow["OWNER"],
			TableName:  ow["TABLE_NAME"],
			ColumnName: ow["COLUMN_NAME"],
		})
	}

	return listData, nil
}

func GatherOracleIndexNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaTableIndexNameLengthCheck, error) {
	synonymInfo, err := oracle.GetOracleTableIndexNameLength(schemaName)
	if err != nil {
		return []ListSchemaTableIndexNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableIndexNameLengthCheck{}, nil
	}

	var listData []ListSchemaTableIndexNameLengthCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaTableIndexNameLengthCheck{
			Schema:    ow["INDEX_OWNER"],
			TableName: ow["TABLE_NAME"],
			IndexName: ow["INDEX_NAME"],
		})
	}

	return listData, nil
}

func GatherOracleViewNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaViewNameLengthCheck, error) {
	synonymInfo, err := oracle.GetOracleTableViewNameLength(schemaName)
	if err != nil {
		return []ListSchemaViewNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaViewNameLengthCheck{}, nil
	}
	t := table.NewWriter()
	t.SetStyle(table.StyleLight)
	t.Style().Title.Align = text.AlignCenter
	t.Style().Options.SeparateRows = true
	t.AppendHeader(table.Row{"Table View Name Length Check", "Table View Name Length Check", "Table View Name Length Check"}, table.RowConfig{AutoMerge: true})

	t.AppendHeader(table.Row{"Schema", "View Name", "Read Only"})

	var listData []ListSchemaViewNameLengthCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaViewNameLengthCheck{
			Schema:   ow["OWNER"],
			ViewName: ow["VIEW_NAME"],
			ReadOnly: ow["READ_ONLY"],
		})
	}

	return listData, nil
}

func GatherOracleSequenceNameLengthCheck(schemaName []string, oracle *oracle.Oracle) ([]ListSchemaSequenceNameLengthCheck, error) {
	synonymInfo, err := oracle.GetOracleTableSequenceNameLength(schemaName)
	if err != nil {
		return []ListSchemaSequenceNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaSequenceNameLengthCheck{}, nil
	}

	var listData []ListSchemaSequenceNameLengthCheck
	for _, ow := range synonymInfo {
		listData = append(listData, ListSchemaSequenceNameLengthCheck{
			Schema:       ow["SEQUENCE_OWNER"],
			SequenceName: ow["SEQUENCE_NAME"],
			OrderFlag:    ow["ORDER_FLAG"],
		})
	}

	return listData, nil
}
