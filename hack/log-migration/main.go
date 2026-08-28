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
	"flag"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"log"
	"os"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
)

var (
	entrypoints     = flag.String("entrypoint", "StaticAutoscaler.RunOnce", "Entrypoint methods")
	pathPattern     = flag.String("path", "./...", "Go package pattern or file path to migrate")
	migrateLogsFlag = flag.Bool("migrate-logs", false, "Migrate logs to structured logging")
)

func isEntry(fn *types.Func, epNames []string) bool {
	name := fn.Name()
	if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
		recvType := sig.Recv().Type()
		if ptr, ok := recvType.(*types.Pointer); ok {
			recvType = ptr.Elem()
		}
		if named, ok := recvType.(*types.Named); ok {
			name = named.Obj().Name() + "." + name
		}
	}
	for _, ep := range epNames {
		if strings.HasSuffix(name, ep) {
			return true
		}
	}
	return false
}

// funcId generates a globally clean identifier for a *types.Func
func funcId(fn *types.Func) string {
	if fn == nil {
		return ""
	}
	s := fn.FullName()
	idx1 := strings.Index(s, " [")
	idx2 := strings.LastIndex(s, "]")
	if idx1 != -1 && idx2 != -1 && idx2 > idx1 {
		s = s[:idx1] + s[idx2+1:]
	}
	return s
}

func getCallIdent(call *ast.CallExpr) *ast.Ident {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun
	case *ast.SelectorExpr:
		return fun.Sel
	case *ast.IndexExpr:
		if ident, ok := fun.X.(*ast.Ident); ok {
			return ident
		} else if sel, ok := fun.X.(*ast.SelectorExpr); ok {
			return sel.Sel
		}
	case *ast.IndexListExpr:
		if ident, ok := fun.X.(*ast.Ident); ok {
			return ident
		} else if sel, ok := fun.X.(*ast.SelectorExpr); ok {
			return sel.Sel
		}
	}
	return nil
}

func hasCtxPrefix(fl *ast.FieldList) bool {
	if fl == nil || len(fl.List) == 0 {
		return false
	}
	if sel, ok := fl.List[0].Type.(*ast.SelectorExpr); ok {
		if id, ok2 := sel.X.(*ast.Ident); ok2 && id.Name == "context" && sel.Sel.Name == "Context" {
			return true
		}
	}
	return false
}

func main() {
	flag.Parse()
	cfg := &packages.Config{Mode: packages.LoadSyntax, Tests: true}
	pkgs, err := packages.Load(cfg, *pathPattern)
	if err != nil {
		log.Fatalf("failed to load packages: %v", err)
	}
	var filteredPkgs []*packages.Package
	for _, pkg := range pkgs {
		if !strings.Contains(pkg.PkgPath, "vendor") && !strings.Contains(pkg.PkgPath, "sdk") {
			filteredPkgs = append(filteredPkgs, pkg)
		}
	}
	pkgs = filteredPkgs

	pf := NewPathfinder()
	epNames := strings.Split(*entrypoints, ",")

	var allFuncs []*types.Func

	for _, pkg := range pkgs {
		// Find all functions and interfaces
		for _, obj := range pkg.TypesInfo.Defs {
			if fn, ok := obj.(*types.Func); ok {
				allFuncs = append(allFuncs, fn)

				if isEntry(fn, epNames) {
					pf.AddSource(funcId(fn))
				}
			}
		}
		// Find all uses of klog
		for _, obj := range pkg.TypesInfo.Uses {
			if fn, ok := obj.(*types.Func); ok {
				if fn.Pkg() != nil && fn.Pkg().Path() == "k8s.io/klog/v2" {
					pf.AddTarget(funcId(fn))
				}
			}
		}
		// Add edge: caller -> callee
		for _, file := range pkg.Syntax {
			var funcStack []*types.Func
			astutil.Apply(file, func(c *astutil.Cursor) bool {
				switch x := c.Node().(type) {
				case *ast.FuncDecl:
					if fn, ok := pkg.TypesInfo.Defs[x.Name].(*types.Func); ok {
						funcStack = append(funcStack, fn)
					} else {
						funcStack = append(funcStack, nil)
					}
				case *ast.CallExpr:
					if len(funcStack) > 0 && funcStack[len(funcStack)-1] != nil {
						caller := funcStack[len(funcStack)-1]
						id := getCallIdent(x)
						if id != nil {
							if callee, ok := pkg.TypesInfo.Uses[id].(*types.Func); ok {
								pf.AddEdge(funcId(caller), funcId(callee))
							}
						}
					}
				}
				return true
			}, func(c *astutil.Cursor) bool {
				if _, ok := c.Node().(*ast.FuncDecl); ok {
					if len(funcStack) > 0 {
						funcStack = funcStack[:len(funcStack)-1]
					}
				}
				return true
			})
		}
	}

	// Link interface methods to concrete implementations safely
	type ifaceMethod struct {
		Iface *types.Interface
		Func  *types.Func
	}
	var ifaceMethods []ifaceMethod
	var concreteFuncs []*types.Func

	for _, fn := range allFuncs {
		sig := fn.Type().(*types.Signature)
		if sig.Recv() != nil {
			if _, isIface := sig.Recv().Type().Underlying().(*types.Interface); isIface {
				ifaceMethods = append(ifaceMethods, ifaceMethod{
					Iface: sig.Recv().Type().Underlying().(*types.Interface),
					Func:  fn,
				})
			} else {
				concreteFuncs = append(concreteFuncs, fn)
			}
		}
	}

	for _, cFn := range concreteFuncs {
		for _, iM := range ifaceMethods {
			if cFn.Name() == iM.Func.Name() {
				recvType := cFn.Type().(*types.Signature).Recv().Type()
				if types.Implements(recvType, iM.Iface) || types.Implements(types.NewPointer(recvType), iM.Iface) {

					pf.AddEdge(funcId(iM.Func), funcId(cFn))
					pf.AddEdge(funcId(cFn), funcId(iM.Func))
				}
			}
		}
	}

	// P calculates bidirectional reachability
	P := pf.OnPaths()

	// Natively rename any legacy 'ctx' parameters that are of type AutoscalingContext
	// to 'autoscalingCtx' across the entire AST to clear the standard 'ctx' namespace.
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(node ast.Node) bool {
				if fd, ok := node.(*ast.FuncDecl); ok {
					if fd.Type.Params != nil {
						for _, param := range fd.Type.Params.List {
							for _, name := range param.Names {
								if name.Name == "ctx" {
									println("found ctx param in AST!")
									if tObj := pkg.TypesInfo.Defs[name]; tObj != nil {
										println("found ctx type: ", tObj.Type().String())
									}
									if tObj := pkg.TypesInfo.Defs[name]; tObj != nil && strings.Contains(tObj.Type().String(), "AutoscalingContext") {
										paramObj := tObj
										name.Name = "autoscalingCtx"
										if fd.Body != nil {
											ast.Inspect(fd.Body, func(inner ast.Node) bool {
												if id, ok2 := inner.(*ast.Ident); ok2 && id.Name == "ctx" && pkg.TypesInfo.Uses[id] == paramObj {
													id.Name = "autoscalingCtx"
												}
												return true
											})
										}
									}
								}
							}
						}
					}
				}
				if it, ok := node.(*ast.InterfaceType); ok {
					if it.Methods != nil {
						for _, method := range it.Methods.List {
							if funcType, ok2 := method.Type.(*ast.FuncType); ok2 {
								if funcType.Params != nil {
									for _, param := range funcType.Params.List {
										for _, name := range param.Names {
											if name.Name == "ctx" {
												println("found ctx param in AST!")
												if tObj := pkg.TypesInfo.Defs[name]; tObj != nil {
													println("found ctx type: ", tObj.Type().String())
												}
												if tObj := pkg.TypesInfo.Defs[name]; tObj != nil && strings.Contains(tObj.Type().String(), "AutoscalingContext") {
													name.Name = "autoscalingCtx"
												}
											}
										}
									}
								}
							}
						}
					}
				}
				return true
			})
		}
	}

	seenFiles := make(map[string]bool)
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			filename := pkg.Fset.Position(file.Pos()).Filename
			if seenFiles[filename] {
				continue
			}
			seenFiles[filename] = true

			needCtxImport := false

			thirdPartyContextRenamed := false
			for _, imp := range file.Imports {
				if imp.Path.Value != `"context"` && strings.HasSuffix(imp.Path.Value, `/context"`) && imp.Name == nil {
					imp.Name = &ast.Ident{Name: "ca_context"}
					thirdPartyContextRenamed = true
				}
			}
			if thirdPartyContextRenamed {
				astutil.Apply(file, func(c *astutil.Cursor) bool {
					if sel, ok := c.Node().(*ast.SelectorExpr); ok {
						if id, ok2 := sel.X.(*ast.Ident); ok2 && id.Name == "context" {
							id.Name = "ca_context"
						}
					}
					return true
				}, nil)
			}

			// Check for third-party context package collisions
			ctxIdent := "context"
			hasStdCtx := false
			for _, imp := range file.Imports {
				if imp.Path.Value == `"context"` {
					hasStdCtx = true
				} else if strings.HasSuffix(imp.Path.Value, `/context"`) && imp.Name == nil {
					ctxIdent = "ca_context"
				}
			}
			_ = hasStdCtx

			modified := false

			var funcStack []*types.Func
			astutil.Apply(file, func(c *astutil.Cursor) bool {
				switch n := c.Node().(type) {
				// replace temporary context with propagated context
				case *ast.AssignStmt:
					if n.Tok == token.DEFINE && len(n.Lhs) == 1 {
						if id, ok := n.Lhs[0].(*ast.Ident); ok && id.Name == "ctx" {
							if len(funcStack) > 0 && funcStack[len(funcStack)-1] != nil && P[funcId(funcStack[len(funcStack)-1])] {
								n.Tok = token.ASSIGN
								modified = true
								if len(n.Rhs) == 1 {
									if call, ok := n.Rhs[0].(*ast.CallExpr); ok {
										if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
											if pkgId, ok := sel.X.(*ast.Ident); ok && (pkgId.Name == "context") {
												if sel.Sel.Name == "TODO" || sel.Sel.Name == "Background" || sel.Sel.Name == "Default" {

													c.Delete()
													modified = true
												}
											}
										}
									}
								}
							}
						}
					}
				// update interfaces to accept context
				case *ast.InterfaceType:
					for _, field := range n.Methods.List {
						if len(field.Names) == 0 {
							continue
						}
						if fn, ok := pkg.TypesInfo.Defs[field.Names[0]].(*types.Func); ok && P[funcId(fn)] {
							if !hasCtxPrefix(field.Type.(*ast.FuncType).Params) {
								hasNames := false
								if len(field.Type.(*ast.FuncType).Params.List) == 0 {
									hasNames = true
								} else {
									for _, param := range field.Type.(*ast.FuncType).Params.List {
										if len(param.Names) > 0 {
											hasNames = true
											break
										}
									}
								}
								ctxField := &ast.Field{
									Type: &ast.SelectorExpr{
										X:   &ast.Ident{Name: ctxIdent},
										Sel: &ast.Ident{Name: "Context"},
									},
								}
								if hasNames {
									ctxField.Names = []*ast.Ident{{Name: "ctx"}}
								}
								field.Type.(*ast.FuncType).Params.List = append([]*ast.Field{ctxField}, field.Type.(*ast.FuncType).Params.List...)
								modified = true
								needCtxImport = true
							}
						}
					}
				// update function declarations to accept context
				case *ast.FuncDecl:
					if fn, ok := pkg.TypesInfo.Defs[n.Name].(*types.Func); ok {
						funcStack = append(funcStack, fn)
						if P[funcId(fn)] && !hasCtxPrefix(n.Type.Params) {
							hasNames := false
							if len(n.Type.Params.List) == 0 {
								hasNames = true
							} else {
								for _, param := range n.Type.Params.List {
									if len(param.Names) > 0 {
										hasNames = true
										break
									}
								}
							}

							ctxField := &ast.Field{
								Type: &ast.SelectorExpr{
									X:   &ast.Ident{Name: ctxIdent},
									Sel: &ast.Ident{Name: "Context"},
								},
							}
							if hasNames {
								ctxField.Names = []*ast.Ident{{Name: "ctx"}}
							}
							n.Type.Params.List = append([]*ast.Field{ctxField}, n.Type.Params.List...)
							modified = true
							needCtxImport = true
						}
					} else {
						funcStack = append(funcStack, nil)
					}
				// add arguments to call expressions
				case *ast.CallExpr:
					id := getCallIdent(n)

					if id != nil {
						if callee, ok := pkg.TypesInfo.Uses[id].(*types.Func); ok {
							// Detect context.With... and replace context.Background() first arg
							if callee.Pkg() != nil && callee.Pkg().Path() == "context" &&
								(callee.Name() == "WithTimeout" || callee.Name() == "WithCancel" ||
									callee.Name() == "WithDeadline" || callee.Name() == "WithValue") {
								if len(n.Args) > 0 {
									if argCall, ok := n.Args[0].(*ast.CallExpr); ok {
										argId := getCallIdent(argCall)
										if argId != nil {
											if argCallee, ok := pkg.TypesInfo.Uses[argId].(*types.Func); ok {
												if argCallee.Pkg() != nil && argCallee.Pkg().Path() == "context" &&
													(argCallee.Name() == "Background" || argCallee.Name() == "TODO") {
													if len(funcStack) > 0 && funcStack[len(funcStack)-1] != nil {
														caller := funcStack[len(funcStack)-1]
														if P[funcId(caller)] {
															n.Args[0] = &ast.Ident{Name: "ctx"}
															modified = true
														}
													}
												}
											}
										}
									}
								}
							}

							if P[funcId(callee)] {
								alreadyPassed := false
								if sig, ok := callee.Type().(*types.Signature); ok && sig.Params().Len() > 0 {
									if strings.HasSuffix(sig.Params().At(0).Type().String(), "context.Context") {
										alreadyPassed = true
									}
								}

								if !alreadyPassed {
									var passExpr ast.Expr = &ast.CallExpr{
										Fun: &ast.SelectorExpr{
											X:   &ast.Ident{Name: ctxIdent},
											Sel: &ast.Ident{Name: "TODO"},
										},
									}

									if len(funcStack) > 0 && funcStack[len(funcStack)-1] != nil {
										caller := funcStack[len(funcStack)-1]
										if P[funcId(caller)] {
											passExpr = &ast.Ident{Name: "ctx"}
										} else {
											needCtxImport = true
										}
									} else {
										needCtxImport = true
									}

									n.Args = append([]ast.Expr{passExpr}, n.Args...)
									modified = true

								} else {
									if len(n.Args) > 0 {
										if call, ok := n.Args[0].(*ast.CallExpr); ok {
											if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
												if pkgId, ok := sel.X.(*ast.Ident); ok && (pkgId.Name == "context") {
													if sel.Sel.Name == "TODO" || sel.Sel.Name == "Background" || sel.Sel.Name == "Default" {
														if len(funcStack) > 0 && funcStack[len(funcStack)-1] != nil {
															caller := funcStack[len(funcStack)-1]
															if P[funcId(caller)] {
																n.Args[0] = &ast.Ident{Name: "ctx"}
																modified = true
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				return true
			}, func(c *astutil.Cursor) bool {
				if _, ok := c.Node().(*ast.FuncDecl); ok {
					if len(funcStack) > 0 {
						funcStack = funcStack[:len(funcStack)-1]
					}
				}
				return true
			})

			if needCtxImport {
				if ctxIdent == "context" {
					astutil.AddImport(pkg.Fset, file, "context")
				} else {
					astutil.AddNamedImport(pkg.Fset, file, ctxIdent, "context")
				}
			}

			if modified {
				var buf bytes.Buffer
				if err := format.Node(&buf, pkg.Fset, file); err == nil {
					stat, err := os.Stat(pkg.Fset.Position(file.Pos()).Filename)
					mode := os.FileMode(0644)
					if err == nil {
						mode = stat.Mode()
					}
					os.WriteFile(pkg.Fset.Position(file.Pos()).Filename, buf.Bytes(), mode)
				}
			}
		}
	}

	if *migrateLogsFlag {
		MigrateLogsForPackages(pkgs)
	}
}
