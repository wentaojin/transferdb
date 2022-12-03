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
package assess

import (
	"github.com/wentaojin/transferdb/errors"
)

func TAssess(assess Assesser) error {
	err := assess.Assess()
	if err != nil {
		return errors.NewMSError(errors.TRANSFERDB, errors.DOMAIN_ASSESS, err)
	}
	return nil
}
