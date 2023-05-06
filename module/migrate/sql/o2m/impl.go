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
	"github.com/wentaojin/transferdb/module/migrate"
)

func IMigrate(ex migrate.Migrator) error {
	err := ex.ReadData()
	if err != nil {
		return err
	}
	err = ex.ProcessData()
	if err != nil {
		return err
	}

	err = ex.ApplyData()
	if err != nil {
		return err
	}
	return nil
}
