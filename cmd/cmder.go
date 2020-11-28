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
package cmd

import (
	"reflect"

	"github.com/spf13/cobra"
)

// Cmder is the interface for objects that form a command hierarchy
type Cmder interface {
	Cmd() *cobra.Command
}

// Cmd traverses a Cmder hierarchy and returns a configured cobra.Command.
// It recursively calls all methods that return a Cmder
// to collect and associate cobra.Command instances.
func Cmd(c Cmder) *cobra.Command {
	return recCmd(c, map[string]bool{})
}

// outTypes is a constant for the array of function output types
var outTypes = []reflect.Type{reflect.TypeOf((*Cmder)(nil)).Elem()}

// recCmd is the actual worker function to visit and collect Cmder instances.
// mmap is for bookkeeping already visted method names.
func recCmd(c Cmder, mmap map[string]bool) *cobra.Command {
	cmd := c.Cmd()
	inV := reflect.ValueOf(c)
	inT := reflect.TypeOf(c)
	funcT := reflect.FuncOf([]reflect.Type{inT}, outTypes, false)
	methods := []reflect.Method{}
	for i := 0; i < inT.NumMethod(); i++ {
		m := inT.Method(i)
		if m.Func.Type() != funcT || mmap[m.Name] {
			continue
		}
		methods = append(methods, m)
	}
	for _, m := range methods {
		mmap[m.Name] = true
	}
	for _, m := range methods {
		subC := m.Func.Call([]reflect.Value{inV})[0].Interface().(Cmder)
		subCmd := recCmd(subC, mmap)
		cmd.AddCommand(subCmd)
	}
	for _, m := range methods {
		mmap[m.Name] = false
	}
	return cmd
}
