package server

// See https://github.com/golang/go/issues/44129#issuecomment-815008489
import (
	// GQLGen generate command dependencies.
	_ "github.com/pkg/errors"
	_ "github.com/urfave/cli/v2"
	_ "golang.org/x/tools/go/ast/astutil"
	_ "golang.org/x/tools/go/packages"
	_ "golang.org/x/tools/imports"
)
