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
func GatherOracleDBOverview(engine *service.Engine, reportName, reportUser string) (*ReportOverview, error) {
	dbName, platformID, platformName, err := engine.GetOracleDBName()
	if err != nil {
		return &ReportOverview{}, err
	}

	globalName, err := engine.GetOracleGlobalName()
	if err != nil {
		return &ReportOverview{}, err
	}
	dbBlockSize, clusterDatabase, CLusterDatabaseInstance, characterSet, err := engine.GetOracleParameters()
	if err != nil {
		return &ReportOverview{}, err
	}

	instanceRes, err := engine.GetOracleInstance()
	if err != nil {
		return &ReportOverview{}, err
	}

	dataSize, err := engine.GetOracleDataTotal()
	if err != nil {
		return &ReportOverview{}, err
	}

	hostCPU, err := engine.GetOracleNumCPU()
	if err != nil {
		return &ReportOverview{}, err
	}
	memorySize, err := engine.GetOracleMemoryGB()
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

func GatherOracleSchemaOverview(schemaName []string, engine *service.Engine) ([]ListSchemaTableSizeData, error) {
	overview, err := engine.GetOracleSchemaOverview(schemaName)
	if err != nil {
		return []ListSchemaTableSizeData{}, err
	}

	if len(overview) == 0 {
		return []ListSchemaTableSizeData{}, err
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

func GatherOracleMaxActiveSessionCount(engine *service.Engine) ([]ListSchemaActiveSession, error) {
	listActiveSession, err := engine.GetOracleMaxActiveSessionCount()
	if err != nil {
		return []ListSchemaActiveSession{}, err
	}

	if len(listActiveSession) == 0 {
		return []ListSchemaActiveSession{}, err
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

func GatherOracleSchemaTableRowsTOP(schemaName []string, engine *service.Engine) ([]ListSchemaTableRowsTOP, error) {
	overview, err := engine.GetOracleSchemaTableRowsTOP(schemaName)
	if err != nil {
		return []ListSchemaTableRowsTOP{}, err
	}
	if len(overview) == 0 {
		return []ListSchemaTableRowsTOP{}, err
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

func GatherOracleSchemaObjectOverview(schemaName []string, engine *service.Engine) ([]ListSchemaTableObjectCounts, error) {
	overview, err := engine.GetOracleObjectTypeOverview(schemaName)
	if err != nil {
		return []ListSchemaTableObjectCounts{}, err
	}

	if len(overview) == 0 {
		return []ListSchemaTableObjectCounts{}, err
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

func GatherOracleSchemaPartitionType(schemaName []string, engine *service.Engine) ([]ListSchemaTablePartitionType, error) {
	partitionInfo, err := engine.GetOraclePartitionObjectType(schemaName)
	if err != nil {
		return []ListSchemaTablePartitionType{}, err
	}

	if len(partitionInfo) == 0 {
		return []ListSchemaTablePartitionType{}, err
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

func GatherOracleSchemaColumnTypeAndMaxLength(schemaName []string, engine *service.Engine) ([]ListSchemaTableColumnTypeAndMaxLength, error) {
	columnInfo, err := engine.GetOracleColumnTypeAndMaxLength(schemaName)
	if err != nil {
		return []ListSchemaTableColumnTypeAndMaxLength{}, err
	}

	if len(columnInfo) == 0 {
		return []ListSchemaTableColumnTypeAndMaxLength{}, err
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

func GatherOracleSchemaTableAvgRowLength(schemaName []string, engine *service.Engine) ([]ListSchemaTableAvgRowLength, error) {
	synonymInfo, err := engine.GetOracleAvgRowLength(schemaName)
	if err != nil {
		return []ListSchemaTableAvgRowLength{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableAvgRowLength{}, err
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

func GatherOracleSchemaTemporaryTable(schemaName []string, engine *service.Engine) ([]ListSchemaTemporaryTableCounts, error) {
	synonymInfo, err := engine.GetOracleTemporaryTable(schemaName)
	if err != nil {
		return []ListSchemaTemporaryTableCounts{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTemporaryTableCounts{}, err
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
func GatherOracleSchemaIndexType(schemaName []string, engine *service.Engine) ([]ListSchemaIndexType, error) {
	columnInfo, err := engine.GetOracleIndexType(schemaName)
	if err != nil {
		return []ListSchemaIndexType{}, err
	}

	if len(columnInfo) == 0 {
		return []ListSchemaIndexType{}, err
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

func GatherOracleConstraintType(schemaName []string, engine *service.Engine) ([]ListSchemaConstraintType, error) {
	columnInfo, err := engine.GetOracleConstraintType(schemaName)
	if err != nil {
		return []ListSchemaConstraintType{}, err
	}

	if len(columnInfo) == 0 {
		return []ListSchemaConstraintType{}, err
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

func GatherOracleSchemeCodeType(schemaName []string, engine *service.Engine) ([]ListSchemaCodeType, error) {
	synonymInfo, err := engine.GetOracleCodeObject(schemaName)
	if err != nil {
		return []ListSchemaCodeType{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaCodeType{}, err
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

func GatherOracleSchemaSynonymType(schemaName []string, engine *service.Engine) ([]ListSchemaSynonymType, error) {
	synonymInfo, err := engine.GetOracleSynonymObjectType(schemaName)
	if err != nil {
		return []ListSchemaSynonymType{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaSynonymType{}, err
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
func GatherOraclePartitionTableCountsCheck(schemaName []string, engine *service.Engine) ([]ListSchemaPartitionTableCountsCheck, error) {
	synonymInfo, err := engine.GetOraclePartitionTableOver1024(schemaName)
	if err != nil {
		return []ListSchemaPartitionTableCountsCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaPartitionTableCountsCheck{}, err
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

func GatherOracleTableRowLengthCheck(schemaName []string, engine *service.Engine) ([]ListSchemaTableRowLengthCheck, error) {
	synonymInfo, err := engine.GetOracleTableRowLengthOver6M(schemaName)
	if err != nil {
		return []ListSchemaTableRowLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableRowLengthCheck{}, err
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

func GatherOracleTableIndexRowLengthCheck(schemaName []string, engine *service.Engine) ([]ListSchemaTableIndexRowLengthCheck, error) {
	synonymInfo, err := engine.GetOracleTableIndexLengthOver3072(schemaName)
	if err != nil {
		return []ListSchemaTableIndexRowLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableIndexRowLengthCheck{}, err
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

func GatherOracleTableColumnCountsCheck(schemaName []string, engine *service.Engine) ([]ListSchemaTableAndIndexCountsCheck, error) {
	synonymInfo, err := engine.GetOracleTableColumnCountsOver512(schemaName)
	if err != nil {
		return []ListSchemaTableAndIndexCountsCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableAndIndexCountsCheck{}, err
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

func GatherOracleTableIndexCountsCheck(schemaName []string, engine *service.Engine) ([]ListSchemaTableAndIndexCountsCheck, error) {
	synonymInfo, err := engine.GetOracleTableIndexCountsOver64(schemaName)
	if err != nil {
		return []ListSchemaTableAndIndexCountsCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableAndIndexCountsCheck{}, err
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

func GatherOracleTableNumberTypeCheck(schemaName []string, engine *service.Engine) ([]ListSchemaTableNumberTypeCheck, error) {
	synonymInfo, err := engine.GetOracleTableNumberTypeCheck(schemaName)
	if err != nil {
		return []ListSchemaTableNumberTypeCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableNumberTypeCheck{}, err
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

func GatherOracleUsernameLengthCheck(schemaName []string, engine *service.Engine) ([]ListUsernameLengthCheck, error) {
	synonymInfo, err := engine.GetOracleUsernameLength(schemaName)
	if err != nil {
		return []ListUsernameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListUsernameLengthCheck{}, err
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

func GatherOracleTableNameLengthCheck(schemaName []string, engine *service.Engine) ([]ListSchemaTableNameLengthCheck, error) {
	synonymInfo, err := engine.GetOracleTableNameLength(schemaName)
	if err != nil {
		return []ListSchemaTableNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableNameLengthCheck{}, err
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

func GatherOracleColumnNameLengthCheck(schemaName []string, engine *service.Engine) ([]ListSchemaTableColumnNameLengthCheck, error) {
	synonymInfo, err := engine.GetOracleTableColumnNameLength(schemaName)
	if err != nil {
		return []ListSchemaTableColumnNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableColumnNameLengthCheck{}, err
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

func GatherOracleIndexNameLengthCheck(schemaName []string, engine *service.Engine) ([]ListSchemaTableIndexNameLengthCheck, error) {
	synonymInfo, err := engine.GetOracleTableIndexNameLength(schemaName)
	if err != nil {
		return []ListSchemaTableIndexNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaTableIndexNameLengthCheck{}, err
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

func GatherOracleViewNameLengthCheck(schemaName []string, engine *service.Engine) ([]ListSchemaViewNameLengthCheck, error) {
	synonymInfo, err := engine.GetOracleTableViewNameLength(schemaName)
	if err != nil {
		return []ListSchemaViewNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaViewNameLengthCheck{}, err
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

func GatherOracleSequenceNameLengthCheck(schemaName []string, engine *service.Engine) ([]ListSchemaSequenceNameLengthCheck, error) {
	synonymInfo, err := engine.GetOracleTableSequenceNameLength(schemaName)
	if err != nil {
		return []ListSchemaSequenceNameLengthCheck{}, err
	}

	if len(synonymInfo) == 0 {
		return []ListSchemaSequenceNameLengthCheck{}, err
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
