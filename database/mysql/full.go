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
	"context"
	"fmt"
	"time"
)

func (m *MySQL) TruncateMySQLTable(targetSchema string, targetTable string) error {
	_, err := m.MySQLDB.ExecContext(m.Ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s", targetSchema, targetTable))
	if err != nil {
		return err
	}
	return nil
}

func (m *MySQL) WriteMySQLTable(sql string) error {
	timeoutCtx, cancel := context.WithTimeout(m.Ctx, 30*time.Second)
	defer cancel()

	_, err := m.MySQLDB.ExecContext(timeoutCtx, sql)
	if err != nil {
		return err
	}
	return nil
}
