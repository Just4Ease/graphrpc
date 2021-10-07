package servergen

import (
	"fmt"
	"github.com/99designs/gqlgen/codegen"
	"github.com/99designs/gqlgen/codegen/templates"
	"github.com/99designs/gqlgen/plugin"
	"github.com/gookit/color"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

func New(filename, modPackageName string) plugin.Plugin {
	return &Plugin{filename, modPackageName}
}

type Plugin struct {
	filename       string
	modPackageName string
}

var _ plugin.CodeGenerator = &Plugin{}

func (m *Plugin) Name() string {
	return "servergen"
}
func (m *Plugin) GenerateCode(data *codegen.Data) error {
	serverBuild := &ServerBuild{
		ExecPackageName:     data.Config.Exec.ImportPath(),
		ResolverPackageName: data.Config.Resolver.ImportPath(),
	}

	if _, err := os.Stat(m.filename); os.IsNotExist(errors.Cause(err)) {
		if err := templates.Render(templates.Options{
			PackageName: "main",
			Filename:    m.filename,
			Data:        serverBuild,
			Packages:    data.Config.Packages,
		}); err != nil {
			return err
		}

		input, err := ioutil.ReadFile(m.filename)
		if err != nil {
			return err
		}

		lines := strings.Split(string(input), "\n")

		reg := regexp.MustCompile("[^A-Za-z0-9]+")
		sanitizedModPackageName := reg.ReplaceAllString(m.modPackageName, "-")
		for i, line := range lines {
			if strings.Contains(line, "#_MOD_PACKAGE_NAME") {
				lines[i] = fmt.Sprintf(`    	ServiceName:         "%s",`, sanitizedModPackageName)
			}
		}
		output := strings.Join(lines, "\n")
		if err := ioutil.WriteFile(m.filename, []byte(output), 0644); err != nil {
			return err
		}

		_, _ = fmt.Fprint(os.Stdout, color.Green.Sprintf("✅  Successfully generated code.\n💡 Exec \"go run ./%s\" to start GraphRPC server\n", m.filename))
	} else {
		color.Yellow.Printf("🦊 Skipped server entrypoint file: %s already exists\n", m.filename)
	}

	return nil
}

type ServerBuild struct {
	codegen.Data

	ExecPackageName     string
	ResolverPackageName string
}
