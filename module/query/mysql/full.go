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
package mysql

import (
	"fmt"
	"go.uber.org/zap"
)

func (m *MySQL) TruncateMySQLTable(targetSchema string, targetTable string) error {
	_, err := m.MySQLDB.ExecContext(m.Ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s", targetSchema, targetTable))
	if err != nil {
		return fmt.Errorf("truncate mysql schema [%v] table [%v] reocrd failed: %v", targetSchema, targetTable, err.Error())
	}
	zap.L().Info("truncate table",
		zap.String("schema", targetSchema),
		zap.String("table", targetTable),
		zap.String("status", "success"))
	return nil
}

func (m *MySQL) WriteMySQLTable(sql string) error {
	_, err := m.MySQLDB.ExecContext(m.Ctx, sql)
	if err != nil {
		return fmt.Errorf("source schema table sql [%v] write failed: %v", sql, err)
	}
	return nil
}
