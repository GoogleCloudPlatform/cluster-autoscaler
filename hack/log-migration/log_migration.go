// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/token"
	"os"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
)

func MigrateLogsForPackages(pkgs []*packages.Package) {
	seenFiles := make(map[string]bool)
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			filename := pkg.Fset.Position(file.Pos()).Filename
			if seenFiles[filename] {
				continue
			}
			seenFiles[filename] = true

			if migrateLogsInFile(pkg, file) {
				var buf bytes.Buffer
				if err := format.Node(&buf, pkg.Fset, file); err == nil {
					stat, err := os.Stat(filename)
					mode := os.FileMode(0644)
					if err == nil {
						mode = stat.Mode()
					}
					os.WriteFile(filename, buf.Bytes(), mode)
				}
			}
		}
	}
}

func migrateLogsInFile(pkg *packages.Package, file *ast.File) bool {
	modifiedFile := false

	type funcContext struct {
		fd       *ast.FuncDecl
		hasCtx   bool
		modified bool
	}
	var stack []*funcContext

	astutil.Apply(file, func(c *astutil.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.FuncDecl:
			fc := &funcContext{fd: n, hasCtx: false, modified: false}
			if n.Type.Params != nil {
				for _, p := range n.Type.Params.List {
					for _, name := range p.Names {
						if name.Name == "ctx" && isContextType(p) {
							fc.hasCtx = true
							break
						}
					}
				}
			}
			stack = append(stack, fc)
		case *ast.CallExpr:
			if len(stack) > 0 {
				fc := stack[len(stack)-1]
				if !fc.hasCtx {
					return true
				}

				isKlog := false
				isV := false
				isErrorf := false
				isInfof := false
				isWarningf := false
				var vLevel ast.Expr

				if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
					if id, ok2 := sel.X.(*ast.Ident); ok2 && id.Name == "klog" {
						isKlog = true
						if sel.Sel.Name == "Errorf" || sel.Sel.Name == "Error" {
							isErrorf = true
						} else if sel.Sel.Name == "Infof" || sel.Sel.Name == "Info" {
							isInfof = true
						} else if sel.Sel.Name == "Warningf" || sel.Sel.Name == "Warning" {
							isWarningf = true
						}
					}
				}
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
					if call, ok2 := sel.X.(*ast.CallExpr); ok2 {
						if innerSel, ok3 := call.Fun.(*ast.SelectorExpr); ok3 {
							if id, ok4 := innerSel.X.(*ast.Ident); ok4 && id.Name == "klog" && innerSel.Sel.Name == "V" {
								isKlog = true
								isV = true
								if len(call.Args) > 0 {
									vLevel = call.Args[0]
								}
								if sel.Sel.Name == "Infof" || sel.Sel.Name == "Info" {
									isInfof = true
								}
							}
						}
					}
				}
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
					if call, ok2 := sel.X.(*ast.CallExpr); ok2 {
						if innerSel, ok3 := call.Fun.(*ast.SelectorExpr); ok3 {
							if id, ok4 := innerSel.X.(*ast.Ident); ok4 && id.Name == "klogx" && innerSel.Sel.Name == "V" {
								isKlog = true
								isV = true
								if len(call.Args) > 1 {
									vLevel = call.Args[1]
								} else if len(call.Args) > 0 {
									vLevel = call.Args[0]
								}
								if sel.Sel.Name == "Infof" || sel.Sel.Name == "Info" {
									isInfof = true
								}
							}
						}
					}
				}

				if isKlog && (isErrorf || isInfof || isWarningf) && len(n.Args) > 0 {
					modifiedFile = true

					var newFun ast.Expr
					baseLogger := &ast.Ident{Name: "logger"}

					if isV && vLevel != nil {
						newFun = &ast.SelectorExpr{
							X: &ast.CallExpr{
								Fun: &ast.SelectorExpr{
									X:   baseLogger,
									Sel: &ast.Ident{Name: "V"},
								},
								Args: []ast.Expr{vLevel},
							},
							Sel: &ast.Ident{Name: "Info"},
						}
					} else if isErrorf {
						newFun = &ast.SelectorExpr{
							X:   baseLogger,
							Sel: &ast.Ident{Name: "Error"},
						}
					} else {
						newFun = &ast.SelectorExpr{
							X:   baseLogger,
							Sel: &ast.Ident{Name: "Info"},
						}
					}

					oldArgs := n.Args
					var newArgs []ast.Expr
					if id, ok := oldArgs[0].(*ast.Ident); ok && id.Name == "ctx" {
						oldArgs = oldArgs[1:]
					}

					var formatStr string
					isBasicLit := false
					if len(oldArgs) > 0 {
						if lit, ok := oldArgs[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
							formatStr = strings.Trim(lit.Value, `"`)
							isBasicLit = true
						}
					}

					var errArg ast.Expr
					if isErrorf && len(oldArgs) > 1 {
						for i := len(oldArgs) - 1; i >= 1; i-- {
							typ := pkg.TypesInfo.TypeOf(oldArgs[i])
							if typ != nil && strings.HasSuffix(typ.String(), "error") {
								errArg = oldArgs[i]
								oldArgs = append(oldArgs[:i], oldArgs[i+1:]...)
								break
							}
						}
						if errArg == nil {
							errArg = &ast.Ident{Name: "nil"}
						}
						newArgs = append(newArgs, errArg)
					} else if isErrorf && len(oldArgs) == 1 {
						newArgs = append(newArgs, &ast.Ident{Name: "nil"})
					}

					if !isBasicLit {
						cmt := &ast.Comment{
							Text:  "// TODO: unable to migrate log message. Please migrate manually.",
							Slash: n.Pos() - 1,
						}
						file.Comments = append(file.Comments, &ast.CommentGroup{List: []*ast.Comment{cmt}})
					}

					fc.modified = true

					if isBasicLit {
						msgStr := extractMessage(formatStr)
						newArgs = append(newArgs, &ast.BasicLit{Kind: token.STRING, Value: `"` + msgStr + `"`})
					} else if len(oldArgs) > 0 {
						newArgs = append(newArgs, oldArgs[0])
					}

					for i := 1; i < len(oldArgs); i++ {
						key := argToKey(oldArgs[i])
						newArgs = append(newArgs, &ast.BasicLit{Kind: token.STRING, Value: `"` + key + `"`})
						newArgs = append(newArgs, oldArgs[i])
					}

					n.Fun = newFun
					n.Args = newArgs
				}
			}
		}
		return true
	}, func(c *astutil.Cursor) bool {
		if fd, ok := c.Node().(*ast.FuncDecl); ok {
			if len(stack) > 0 {
				fc := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if fc.modified && fd.Body != nil {
					stmt := &ast.AssignStmt{
						Lhs: []ast.Expr{&ast.Ident{Name: "logger"}},
						Tok: token.DEFINE,
						Rhs: []ast.Expr{
							&ast.CallExpr{
								Fun: &ast.SelectorExpr{
									X:   &ast.Ident{Name: "klog"},
									Sel: &ast.Ident{Name: "FromContext"},
								},
								Args: []ast.Expr{&ast.Ident{Name: "ctx"}},
							},
						},
					}
					fd.Body.List = append([]ast.Stmt{stmt}, fd.Body.List...)
				}
			}
		}
		return true
	})

	return modifiedFile
}

func isContextType(field *ast.Field) bool {
	if sel, ok := field.Type.(*ast.SelectorExpr); ok {
		if id, ok2 := sel.X.(*ast.Ident); ok2 &&
			id.Name == "context" && sel.Sel.Name == "Context" {
			return true
		}
	}
	return false
}

func extractMessage(format string) string {
	idx := strings.Index(format, "%")
	if idx > 0 {
		return strings.TrimSpace(format[:idx])
	}
	if format == "" {
		return "log"
	}
	return format
}

func argToKey(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	case *ast.CallExpr:
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok {
			return sel.Sel.Name
		}
		if id, ok := e.Fun.(*ast.Ident); ok {
			return id.Name
		}
	}
	return "arg"
}
