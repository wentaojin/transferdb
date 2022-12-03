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
	"embed"
	"fmt"
	"os"
	"text/template"
)

//go:embed template
var fs embed.FS

func GenNewHTMLReport(
	reportOverview *ReportOverview, reportSchema *ReportSchema,
	reportType *ReportType, reportCheck *ReportCheck, file *os.File) error {
	tf, err := template.ParseFS(fs, "template/*.html")
	if err != nil {
		return fmt.Errorf("template parse FS failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_header", nil); err != nil {
		return fmt.Errorf("template FS Execute [report_header] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_body", nil); err != nil {
		return fmt.Errorf("template FS Execute [report_body] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_overview", reportOverview); err != nil {
		return fmt.Errorf("template FS Execute [report_overview] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_index", nil); err != nil {
		return fmt.Errorf("template FS Execute [report_index] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_schema", reportSchema); err != nil {
		return fmt.Errorf("template FS Execute [report_schema] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_type", reportType); err != nil {
		return fmt.Errorf("template FS Execute [report_type] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_check", reportCheck); err != nil {
		return fmt.Errorf("template FS Execute [report_check] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_footer", nil); err != nil {
		return fmt.Errorf("template FS Execute [report_footer] template HTML failed: %v", err)
	}

	return nil
}
