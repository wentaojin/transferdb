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
	"context"
	"github.com/wentaojin/transferdb/common"
	"github.com/wentaojin/transferdb/database/meta"
	"github.com/wentaojin/transferdb/database/oracle"
	"github.com/wentaojin/transferdb/module/assess/oracle/public"
)

/*
Oracle Database Overview
*/
func GetAssessDatabaseOverviewResult(ctx context.Context, metaDB *meta.Meta, oracle *oracle.Oracle, reportName, reportUser, dbTypeS, dbTypeT string) (*public.ReportOverview, public.ReportSummary, error) {
	// 获取自定义兼容性内容
	compatibles, err := meta.NewBuildinObjectCompatibleModel(metaDB).BatchQueryObjAssessCompatible(ctx, &meta.BuildinObjectCompatible{
		DBTypeS: dbTypeS,
		DBTypeT: dbTypeT,
	})
	if err != nil {
		return nil, public.ReportSummary{}, err
	}
	objAssessCompsMap := make(map[string]meta.BuildinObjectCompatible)
	for _, c := range compatibles {
		objAssessCompsMap[common.StringUPPER(c.ObjectNameS)] = c
	}

	overview, rs, err := AssessOracleDBOverview(oracle, objAssessCompsMap, reportName, reportUser)
	if err != nil {
		return nil, public.ReportSummary{}, err
	}

	return overview, rs, nil
}
