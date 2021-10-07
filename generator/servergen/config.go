package servergen

import (
	"bytes"
	"text/template"
)

var configTemplate = template.Must(template.New("gqlgen.yml").Parse(`
# Where are all the schema files located? globs are supported eg  src/**/*.graphqls
schema:
  - graph/schemas/*.graphql

# Where should the generated server code go?
exec:
  filename: graph/generated.go
  package: graph

# Uncomment to enable federation
# federation:
#   filename: graph/generated/federation.go
#   package: generated

# Where should any generated models go?
model:
  filename: graph/types.go
  package: graph

# Where should the resolver implementations go?
resolver:
  layout: follow-schema
  dir: graph
  package: graph

# Optional: turn on use ` + "`" + `gqlgen:"fieldName"` + "`" + ` tags in your models
# struct_tag: json

# Optional: turn on to use []Thing instead of []*Thing
# omit_slice_element_pointers: true

# Optional: set to speed up generation time by not performing a final validation pass.
# skip_validation: true

# gqlgen will search for any type names in the schema in these go packages
# if they match it will use them, otherwise it will generate them.
autobind:
  - "{{.}}/graph"

# This section declares type mapping between the GraphQL and go type systems
#
# The first line in each type will be used as defaults for resolver arguments and
# modelgen, the others will be allowed when binding to fields. Configure them to
# your liking
models:
  Int:
    model:
      - github.com/99designs/gqlgen/graphql.Int64
`))

func InitConfig(pkgName string) ([]byte, error) {
	var buf bytes.Buffer
	if err := configTemplate.Execute(&buf, pkgName); err != nil {
		panic(err)
	}

	return buf.Bytes(), nil
}
