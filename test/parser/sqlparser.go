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
	"fmt"
	"strings"

	"github.com/pingcap/parser/format"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	return &stmtNodes[0], nil
}

func main() {
	sql := "insert into MARVIN.MARVIN6(ID,INC_DATETIME,RANDOM_ID,RANDOM_STRING) values (1,TIMESTAMP ' 2021-03-16 18:11:21',1,'pingcap')"
	astNode, err := parse(sql)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	info := extract(astNode)
	fmt.Printf("%v\n", info)

}
func extract(rootNode *ast.StmtNode) *Info {
	v := &Info{}
	(*rootNode).Accept(v)
	return v
}

type Info struct {
	schema    string
	table     string
	operation string
	data      map[string]interface{}
	before    map[string]interface{}
	whereExpr string
	columns   []string
}

func (v *Info) Enter(in ast.Node) (ast.Node, bool) {
	if node, ok := in.(*ast.TableName); ok {
		v.schema = fmt.Sprintf("`%s`", strings.ToUpper(node.Schema.String()))
		v.table = fmt.Sprintf("`%s`", strings.ToUpper(node.Name.String()))
	}

	if node, ok := in.(*ast.UpdateStmt); ok {
		v.operation = "UPDATE"
		v.data = make(map[string]interface{}, 1)
		v.before = make(map[string]interface{}, 1)

		// Set 修改值 -> data
		for _, val := range node.List {
			var sb strings.Builder
			flags := format.DefaultRestoreFlags
			err := val.Expr.Restore(format.NewRestoreCtx(flags, &sb))
			if err != nil {
				fmt.Println(err)
			}
		}
		// 如果存在 WHERE 条件 -> before
		if node.Where != nil {
			if node, ok := node.Where.Accept(v); ok {
				if exprNode, ok := node.(ast.ExprNode); ok {
					var sb strings.Builder
					flags := format.DefaultRestoreFlags
					err := exprNode.Restore(format.NewRestoreCtx(flags, &sb))
					if err != nil {
						fmt.Println(err)
					}
					v.whereExpr = fmt.Sprintf("WHERE %v", sb.String())
				}
			}

			beforeData(node.Where, v.before)
		}

	}
	if node, ok := in.(*ast.InsertStmt); ok {
		v.operation = "INSERT"
		v.data = make(map[string]interface{}, 1)
		for i, col := range node.Columns {
			v.columns = append(v.columns, fmt.Sprintf("`%v`", strings.ToUpper(col.String())))
			for _, lists := range node.Lists {
				var sb strings.Builder
				flags := format.DefaultRestoreFlags
				err := lists[i].Restore(format.NewRestoreCtx(flags, &sb))
				if err != nil {
					fmt.Println(err)
				}
				v.data[fmt.Sprintf("`%s`", col.String())] = sb.String()
			}
		}

	}

	if node, ok := in.(*ast.DeleteStmt); ok {
		v.operation = "DELETE"
		v.before = make(map[string]interface{}, 1)
		// 如果存在 WHERE 条件 -> before
		if node.Where != nil {
			if node, ok := node.Where.Accept(v); ok {
				if exprNode, ok := node.(ast.ExprNode); ok {
					var sb strings.Builder
					flags := format.DefaultRestoreFlags
					err := exprNode.Restore(format.NewRestoreCtx(flags, &sb))
					if err != nil {
						fmt.Println(err)
					}
					v.whereExpr = fmt.Sprintf("WHERE %v", sb.String())
				}
			}
			beforeData(node.Where, v.before)
		}

	}

	if _, ok := in.(*ast.TruncateTableStmt); ok {
		v.operation = "TRUNCATE"
	}

	if _, ok := in.(*ast.DropTableStmt); ok {
		v.operation = "DROP"
	}
	return in, false
}

func (v *Info) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func beforeData(where ast.ExprNode, before map[string]interface{}) {
	if binaryNode, ok := where.(*ast.BinaryOperationExpr); ok {
		switch binaryNode.Op.String() {
		case ast.LogicAnd:
			beforeData(binaryNode.L, before)
			beforeData(binaryNode.R, before)
		case ast.EQ:
			var value strings.Builder
			var column strings.Builder
			flags := format.DefaultRestoreFlags
			err := binaryNode.R.Restore(format.NewRestoreCtx(flags, &value))
			if err != nil {
				fmt.Println(err)
			}
			err = binaryNode.L.Restore(format.NewRestoreCtx(flags, &column))
			if err != nil {
				fmt.Println(err)
			}
			before[strings.ToUpper(column.String())] = value.String()
		}
	}
}
