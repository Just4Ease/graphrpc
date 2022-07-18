package clientgen

import (
	"fmt"
	"github.com/99designs/gqlgen/codegen/templates"
	"github.com/vektah/gqlparser/v2/ast"
	"go/constant"
	"go/types"
)

func (r *SourceGenerator) genFromDefinition(def *ast.Definition) types.Type {
	switch def.Kind {
	case ast.InputObject:
		if r.ccfg.Client.InputAsMap || r.ccfg.Models[def.Name].AsMap {
			genType := r.GetGenType(NewFieldPath(def.Kind, def.Name).Name())

			for _, field := range def.Fields {
				fieldDef := r.cfg.Schema.Types[field.Type.Name()]

				typ := r.namedType(NewFieldPath(fieldDef.Kind, fieldDef.Name), func() types.Type {
					return r.genFromDefinition(fieldDef)
				})

				typ = r.binder.CopyModifiersFromAst(field.Type, typ)

				f := MapField{
					Name: field.Name,
					Type: typ,
				}

				if field.Type.NonNull {
					genType.MapReq = append(genType.MapReq, f)
				} else {
					r.collectPtrTypes(typ, false)

					genType.MapOpt = append(genType.MapOpt, f)
				}
			}

			return types.NewMap(
				types.Typ[types.String],
				types.NewInterfaceType(nil, nil),
			)
		}

		fallthrough // Not input as map, treat as object
	case ast.Object:
		vars := make([]*types.Var, 0, len(def.Fields))
		tags := make([]string, 0, len(def.Fields))

		for _, field := range def.Fields {
			fieldDef := r.cfg.Schema.Types[field.Type.Name()]

			typ := r.namedType(NewFieldPath(fieldDef.Kind, fieldDef.Name), func() types.Type {
				return r.genFromDefinition(fieldDef)
			})

			typ = r.binder.CopyModifiersFromAst(field.Type, typ)

			if isStruct(typ) && (fieldDef.Kind == ast.Object || fieldDef.Kind == ast.InputObject) {
				typ = types.NewPointer(typ)
			}

			name := field.Name
			if nameOveride := r.cfg.Models[def.Name].Fields[field.Name].FieldName; nameOveride != "" {
				name = nameOveride
			}

			vars = append(vars, types.NewVar(0, nil, templates.ToGo(name), typ))
			tags = append(tags, `json:"`+name+`"`)
		}

		return types.NewStruct(vars, tags)

	case ast.Enum:
		genType := r.GetGenType(NewFieldPath(def.Kind, def.Name).Name())

		consts := make([]*types.Const, 0)
		for _, v := range def.EnumValues {
			consts = append(consts, types.NewConst(
				0,
				r.client.Pkg(),
				fmt.Sprintf("%v%v", templates.ToGo(def.Name), templates.ToGo(v.Name)),
				genType.RefType,
				constant.MakeString(v.Name),
			))
		}

		genType.Consts = consts

		return types.Typ[types.String]
	case ast.Scalar:
		panic("scalars must be predeclared: " + def.Name)
	}

	panic("cannot generate type for def: " + def.Name)
}

func isStruct(t types.Type) bool {
	_, is := t.Underlying().(*types.Struct)
	return is
}
