// Wrapper around x/tools/imports that only removes imports, never adds new ones.

package imports

import (
	"github.com/borderlesshq/graphrpc/internal/code"
	"go/ast"
	"strings"
)

type visitFn func(node ast.Node)

func (fn visitFn) Visit(node ast.Node) ast.Visitor {
	fn(node)
	return fn
}

func getUnusedImports(file ast.Node, packages *code.Packages) map[string]string {
	imported := map[string]*ast.ImportSpec{}
	used := map[string]bool{}

	ast.Walk(visitFn(func(node ast.Node) {
		if node == nil {
			return
		}
		switch v := node.(type) {
		case *ast.ImportSpec:
			if v.Name != nil {
				imported[v.Name.Name] = v
				break
			}
			ipath := strings.Trim(v.Path.Value, `"`)
			if ipath == "C" {
				break
			}

			local := packages.NameForPackage(ipath)

			imported[local] = v
		case *ast.SelectorExpr:
			xident, ok := v.X.(*ast.Ident)
			if !ok {
				break
			}
			if xident.Obj != nil {
				// if the parser can resolve it, it's not a package ref
				break
			}
			used[xident.Name] = true
		}
	}), file)

	for pkg := range used {
		delete(imported, pkg)
	}

	unusedImport := map[string]string{}
	for pkg, is := range imported {
		if !used[pkg] && pkg != "_" && pkg != "." {
			name := ""
			if is.Name != nil {
				name = is.Name.Name
			}
			unusedImport[strings.Trim(is.Path.Value, `"`)] = name
		}
	}

	return unusedImport
}
