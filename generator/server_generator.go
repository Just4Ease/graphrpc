package generator

import (
	"fmt"
	"github.com/borderlesshq/graphrpc/generator/servergen"
	"github.com/borderlesshq/graphrpc/internal/code"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/api"
	genCfg "github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/codegen/config"
	"github.com/borderlesshq/graphrpc/utils"
	"github.com/gookit/color"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

func GenerateGraphRPCServer(fileName string) {

	pkgName := code.ImportPathForDir(".")
	if pkgName == "" {
		_, _ = fmt.Fprintln(os.Stderr, "unable to determine import path for current directory, you probably need to run go mod init first\"")
		os.Exit(4)
		return
	}

	cfg, err := servergen.LoadConfigFromDefaultLocations()
	if err != nil {
		//if hasGeneratedFile {
		//	_, _ = fmt.Fprintln(os.Stderr, errors.Wrap(err, "unable to parse config").Error())
		//	os.Exit(4)
		//	return
		//}
		configByte, err := servergen.InitConfig(pkgName)
		if err != nil {
			return
		}
		if err = ioutil.WriteFile("gqlgen.yml", configByte, os.ModePerm); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, errors.Wrap(err, "unable to generate & parse config").Error())
			os.Exit(4)
			return
		}

		cfg = genCfg.DefaultConfig()
		if err := yaml.UnmarshalStrict(configByte, cfg); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, errors.Wrap(err, "unable to parse config").Error())
			os.Exit(4)
			return
		}

		if err := servergen.PrepareSchema("graph/schemas/"); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(4)
			return
		}
	}

	//configByte, err := servergen.InitConfig(pkgName)
	//if err != nil {
	//	return
	//}
	//
	//cfg := genCfg.DefaultConfig()

	_, _ = fmt.Fprint(os.Stdout, color.Green.Sprint("✅  Successfully generated resolvers.\n"))

	serverGenPlugin := api.AddPlugin(servergen.New(fileName, pkgName))
	//modelGenPlugin := ""
	if err := api.Generate(cfg, serverGenPlugin); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(4)
		return
	}

	utils.FixFieldAlignment(cfg.Model.Dir())
}
