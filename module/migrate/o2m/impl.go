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

import "github.com/wentaojin/transferdb/module/migrate"

func IExtractor(e migrate.Extractor) ([]string, []string, error) {
	columnFields, batchResults, err := e.GetTableRows()
	if err != nil {
		return columnFields, batchResults, err
	}
	return columnFields, batchResults, nil
}

func ITranslator(t migrate.Translator) error {
	err := t.TranslateTableRows()
	if err != nil {
		return err
	}
	return nil
}

func IApplier(r migrate.Applier) error {
	err := r.ApplyTableRows()
	if err != nil {
		return err
	}
	return nil
}
