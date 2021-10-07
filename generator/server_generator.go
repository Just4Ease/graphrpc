package generator

import (
	"fmt"
	"github.com/99designs/gqlgen/api"
	genCfg "github.com/99designs/gqlgen/codegen/config"
	"github.com/Just4Ease/graphrpc/generator/servergen"
	"github.com/Just4Ease/graphrpc/internal/code"
	"github.com/gookit/color"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"os"
)

func GenerateGraphRPCServer(fileName string) {

	pkgName := code.ImportPathForDir(".")
	if pkgName == "" {
		_, _ = fmt.Fprintln(os.Stderr, "unable to determine import path for current directory, you probably need to run go mod init first\"")
		os.Exit(4)
		return
	}

	configByte, err := servergen.InitConfig(pkgName)
	if err != nil {
		return
	}

	cfg := genCfg.DefaultConfig()

	if err := yaml.UnmarshalStrict(configByte, cfg); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, errors.Wrap(err, "unable to parse config").Error())
		os.Exit(4)
		return
	}

	if err := genCfg.CompleteConfig(cfg); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(4)
		return
	}

	if err := servergen.PrepareSchema("graph/schemas/"); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(4)
		return
	}

	_, _ = fmt.Fprint(os.Stdout, color.Green.Sprint("✅  Successfully generated resolvers.\n"))

	if err := api.Generate(cfg, api.AddPlugin(servergen.New(fileName, pkgName))); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(4)
		return
	}
}
