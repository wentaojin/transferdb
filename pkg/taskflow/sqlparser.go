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
package taskflow

import (
	"encoding/json"
	"strings"

	"github.com/wentaojin/transferdb/utils"

	"go.uber.org/zap"

	"github.com/pingcap/parser/format"

	"github.com/pingcap/parser"

	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func parseSQL(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	return &stmtNodes[0], nil
}

func extractStmt(rootNode *ast.StmtNode) *Stmt {
	v := &Stmt{}
	(*rootNode).Accept(v)
	return v
}

type Stmt struct {
	Schema    string
	Table     string
	Columns   []string
	Operation string
	Data      map[string]interface{}
	Before    map[string]interface{}
	WhereExpr string
}

// WARNING: sql parser Format() has be discrepancy ,be is instead of Restore()
func (v *Stmt) Enter(in ast.Node) (ast.Node, bool) {
	if node, ok := in.(*ast.TableName); ok {
		v.Schema = utils.StringsBuilder("`", strings.ToUpper(node.Schema.String()), "`")
		v.Table = utils.StringsBuilder("`", strings.ToUpper(node.Name.String()), "`")
	}

	if node, ok := in.(*ast.UpdateStmt); ok {
		v.Operation = "UPDATE"
		v.Data = make(map[string]interface{}, 1)
		v.Before = make(map[string]interface{}, 1)

		// Set 修改值 -> data
		for _, val := range node.List {
			var sb strings.Builder
			flags := format.DefaultRestoreFlags
			err := val.Expr.Restore(format.NewRestoreCtx(flags, &sb))
			if err != nil {
				zap.L().Error("sql parser failed",
					zap.String("stmt", v.Marshal()))
			}
		}

		// 如果存在 WHERE 条件 -> before
		if node.Where != nil {
			if node, ok := node.Where.Accept(v); ok {
				if exprNode, ok := node.(ast.ExprNode); ok {
					var sb strings.Builder
					sb.WriteString("WHERE ")
					flags := format.DefaultRestoreFlags
					err := exprNode.Restore(format.NewRestoreCtx(flags, &sb))
					if err != nil {
						zap.L().Error("sql parser failed",
							zap.String("stmt", v.Marshal()))
					}
					v.WhereExpr = sb.String()
				}
			}
			beforeData(node.Where, v.Before)
		}

	}

	if node, ok := in.(*ast.InsertStmt); ok {
		v.Operation = "INSERT"
		v.Data = make(map[string]interface{}, 1)
		for i, col := range node.Columns {
			v.Columns = append(v.Columns, utils.StringsBuilder("`", strings.ToUpper(col.String()), "`"))
			for _, lists := range node.Lists {
				var sb strings.Builder
				flags := format.DefaultRestoreFlags
				err := lists[i].Restore(format.NewRestoreCtx(flags, &sb))
				if err != nil {
					zap.L().Error("sql parser failed",
						zap.String("stmt", v.Marshal()))
				}
				v.Data[utils.StringsBuilder("`", strings.ToUpper(col.String()), "`")] = sb.String()
			}
		}
	}

	if node, ok := in.(*ast.DeleteStmt); ok {
		v.Operation = "DELETE"
		v.Before = make(map[string]interface{}, 1)
		// 如果存在 WHERE 条件 -> before
		if node.Where != nil {
			if node, ok := node.Where.Accept(v); ok {
				if exprNode, ok := node.(ast.ExprNode); ok {
					var sb strings.Builder
					sb.WriteString("WHERE ")
					flags := format.DefaultRestoreFlags
					err := exprNode.Restore(format.NewRestoreCtx(flags, &sb))
					if err != nil {
						zap.L().Error("sql parser failed",
							zap.String("stmt", v.Marshal()))
					}
					v.WhereExpr = sb.String()
				}
			}
			beforeData(node.Where, v.Before)
		}
	}

	if _, ok := in.(*ast.TruncateTableStmt); ok {
		v.Operation = "TRUNCATE"
	}
	if _, ok := in.(*ast.DropTableStmt); ok {
		v.Operation = "DROP"
	}
	return in, false
}

func (v *Stmt) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (v *Stmt) Marshal() string {
	b, err := json.Marshal(&v)
	if err != nil {
		zap.L().Error("marshal stmt to string",
			zap.String("string", string(b)),
			zap.Error(err))
	}
	return string(b)
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
				zap.L().Error("sql parser failed",
					zap.String("error", err.Error()))
			}
			err = binaryNode.L.Restore(format.NewRestoreCtx(flags, &column))
			if err != nil {
				zap.L().Error("sql parser failed",
					zap.String("error", err.Error()))
			}
			before[strings.ToUpper(column.String())] = value.String()
		}
	}
}
