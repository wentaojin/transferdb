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
package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/WentaoJin/tidba/cmd"
	"github.com/WentaoJin/tidba/zlog"
)

func init() {
	if err := zlog.NewZapLogger(); err != nil {
		log.Fatalf("New global zap logger failed: %v", err)
	}
}

func main() {
	go func() {
		if err := http.ListenAndServe(":9696", nil); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}()
	app := &cmd.App{}
	cmder := cmd.Cmd(app)
	if err := cmder.Execute(); err != nil {
		log.Fatalln(err)
	}
}
