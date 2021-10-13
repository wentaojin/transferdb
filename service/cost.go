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
package service

func (e *Engine) GetOracleDBName() (string, string, string, error) {
	_, res, err := Query(e.OracleDB, `SELECT name dbname,platform_id platform_id,platform_name platform_name FROM v$database`)
	if err != nil {
		return "", "", "", err
	}
	return res[0]["DBNAME"], res[0]["PLATFORM_ID"], res[0]["PLATFORM_NAME"], nil
}

func (e *Engine) GetOracleGlobalName() (string, error) {
	_, res, err := Query(e.OracleDB, `SELECT global_name global_name FROM global_name`)
	if err != nil {
		return "", err
	}
	return res[0]["GLOBAL_NAME"], nil
}

func (e *Engine) GetOracleParameters() (string, string, string, string, error) {
	var (
		dbBlockSize             string
		clusterDatabase         string
		CLusterDatabaseInstance string
		timedStatistics         string
	)
	_, res, err := Query(e.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'db_block_size'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, timedStatistics, err
	}
	dbBlockSize = res[0]["VALUE"]

	_, res, err = Query(e.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'cluster_database'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, timedStatistics, err
	}
	clusterDatabase = res[0]["VALUE"]

	_, res, err = Query(e.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'cluster_database_instances'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, timedStatistics, err
	}
	CLusterDatabaseInstance = res[0]["VALUE"]

	_, res, err = Query(e.OracleDB, `SELECT VALUE FROM v$parameter WHERE	NAME = 'timed_statistics'`)
	if err != nil {
		return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, timedStatistics, err
	}
	timedStatistics = res[0]["VALUE"]

	return dbBlockSize, clusterDatabase, CLusterDatabaseInstance, timedStatistics, nil
}
