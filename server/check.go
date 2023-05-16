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
package server

import (
	"context"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/config"
	"github.com/wentaojin/transferdb/module/check"
	"github.com/wentaojin/transferdb/module/check/m2o"
	"github.com/wentaojin/transferdb/module/check/o2m"
	"strings"
)

func ICheck(ctx context.Context, cfg *config.Config) error {
	var (
		r   check.Reporter
		err error
	)
	switch {
	case strings.EqualFold(cfg.DBTypeS, common.DatabaseTypeOracle) && strings.EqualFold(cfg.DBTypeT, common.DatabaseTypeMySQL):
		r, err = o2m.NewCheck(ctx, cfg)
		if err != nil {
			return err
		}
	case strings.EqualFold(cfg.DBTypeS, common.DatabaseTypeMySQL) && strings.EqualFold(cfg.DBTypeT, common.DatabaseTypeOracle):
		r, err = m2o.NewCheck(ctx, cfg)
		if err != nil {
			return err
		}
	}
	err = r.Check()
	if err != nil {
		return err
	}
	return nil
}
