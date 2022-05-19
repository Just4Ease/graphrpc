package generator

import (
	"context"
	"errors"
	"fmt"
	"github.com/Just4Ease/axon/v2"
	"github.com/borderlesshq/graphrpc/client"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/api"
	genCfg "github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/codegen/config"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/plugin"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/plugin/modelgen"
	"github.com/borderlesshq/graphrpc/libs/Yamashou/gqlgenc/clientgen"
	"github.com/borderlesshq/graphrpc/libs/Yamashou/gqlgenc/config"
	"github.com/borderlesshq/graphrpc/utils"
	"github.com/gookit/color"
	"os"
	"path"
	"strings"
)

type ClientGenerator struct {
	PackageName                  string
	PackagePath                  string
	RemoteServiceName            string
	RemoteServiceGraphEntrypoint string
	customTypes                  []customType
	QueriesPath                  string
	QueryParamsPrefix            string
	QueryParamsSuffix            string
	MutationParamsPrefix         string
	MutationParamsSuffix         string
	Headers                      map[string]string
	ClientV2                     bool
	cfg                          *config.Config
	Conn                         axon.EventStore
}

type customType struct {
	name  string
	model string
}

type ClientGeneratorOption func(*ClientGenerator) error

// Package is an Option to set the generated client's package name and directory where it will be saved
func Package(name string, directory string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		o.PackageName = name
		o.PackagePath = directory
		return nil
	}
}

// QueriesPath is an Option to set the path where user's queries are stored to generate methods for.
func QueriesPath(directory string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		o.QueriesPath = directory
		return nil
	}
}

// GeneratedQueriesPrefix is an Option to set prefix of your generated queries input params.
func GeneratedQueriesPrefix(prefix string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		o.QueryParamsPrefix = prefix
		return nil
	}
}

// GeneratedQueriesSuffix is an Option to set suffix of your generated mutations input params.
func GeneratedQueriesSuffix(suffix string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		o.QueryParamsSuffix = suffix
		return nil
	}
}

// GeneratedMutationsPrefix is an Option to set prefix of your generated mutations' input params.
func GeneratedMutationsPrefix(prefix string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		o.MutationParamsPrefix = prefix
		return nil
	}
}

// GeneratedMutationsSuffix is an Option to set suffix of your generated mutations' input params.
func GeneratedMutationsSuffix(suffix string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		o.MutationParamsSuffix = suffix
		return nil
	}
}

// RemoteServiceName is used to mount the . example: "/graphql"
func RemoteServiceName(remoteServiceName string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		o.RemoteServiceName = remoteServiceName
		return nil
	}
}

// RemoteGraphQLPath is an Option to set RemoteServiceGraphEntrypoint used for introspection. example: "/graphql"
func RemoteGraphQLPath(path string, headers map[string]string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		if path[:1] == "/" {
			path = path[1:]
		}
		o.RemoteServiceGraphEntrypoint = path
		o.Headers = headers
		return nil
	}
}

// SetAxonConn is an Option to set axon connection. See https://github.com/Just4Ease/axon
func SetAxonConn(conn axon.EventStore) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		if conn == nil {
			return errors.New("axon connection must not be nil. see https://github.com/Just4Ease/axon")
		}

		o.Conn = conn
		return nil
	}
}

// RegisterCustomModelTypes is an Option to set suffix of your generated mutations' input params.
func RegisterCustomModelTypes(typeName, model string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		//if o.customTypes == nil {
		//
		//}
		o.customTypes = append(o.customTypes, customType{
			name:  typeName,
			model: model,
		})
		return nil
	}
}

type Clients struct {
	generateToDirectory string
	list                []*ClientGenerator
}

func NewClientGenerator(generateToDirectory string) *Clients {
	list := make([]*ClientGenerator, 0)
	return &Clients{list: list, generateToDirectory: generateToDirectory}
}

func (c *Clients) AddClient(opts ...ClientGeneratorOption) error {
	clientGenerator := &ClientGenerator{}

	for _, opt := range opts {
		if err := opt(clientGenerator); err != nil {
			return err
		}
	}

	query := make([]string, 0)
	if strings.TrimSpace(clientGenerator.QueriesPath) != "" {
		q := strings.Split(clientGenerator.QueriesPath, ",")
		for _, s := range q {
			cleanedPath := path.Clean(fmt.Sprintf("%s/%s/%s", c.generateToDirectory, clientGenerator.PackagePath, s))
			query = append(query, cleanedPath)
		}
	}

	model := path.Clean(fmt.Sprintf("%s/%s/types.go", c.generateToDirectory, clientGenerator.PackagePath))
	generated := path.Clean(fmt.Sprintf("%s/%s/generated.go", c.generateToDirectory, clientGenerator.PackagePath))

	cfgParams := &config.Config{
		//SchemaFilename: schema,
		Model: genCfg.PackageConfig{
			Filename: model,
			Package:  clientGenerator.PackageName,
		},
		Client: genCfg.PackageConfig{
			Filename: generated,
			Package:  clientGenerator.PackageName,
		},
		//Models: nil,
		Endpoint: &config.EndPointConfig{
			URL:     clientGenerator.RemoteServiceGraphEntrypoint,
			Headers: clientGenerator.Headers,
		},
		Generate: nil,
		Query:    query,
	}

	if clientGenerator.ClientV2 {
		cfgParams.Generate = &config.GenerateConfig{
			Prefix: &config.NamingConfig{
				Query:    clientGenerator.QueryParamsPrefix,
				Mutation: clientGenerator.MutationParamsPrefix,
			},
			Suffix: &config.NamingConfig{
				Query:    clientGenerator.QueryParamsSuffix,
				Mutation: clientGenerator.MutationParamsSuffix,
			},
			Client:   nil,
			ClientV2: true,
		}
	}

	cfg, err := config.LoadClientGeneratorCfg(cfgParams)
	if err != nil {
		return err
	}

	clientGenerator.cfg = cfg
	c.list = append(c.list, clientGenerator)
	return nil
}

func (c *Clients) Generate() {
	ctx := context.Background()
	for _, g := range c.list {
		clientGen := api.AddPlugin(clientgen.New(g.cfg.Query, g.cfg.Client, g.cfg.Generate, g.RemoteServiceName, g.RemoteServiceGraphEntrypoint))
		if err := generateClientCode(ctx, g, clientGen); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[%s] %+v", g.RemoteServiceName, err.Error())
			os.Exit(4)
		}

		model := path.Clean(fmt.Sprintf("%s/%s/types.go", c.generateToDirectory, g.PackagePath))
		//generated := path.Clean(fmt.Sprintf("%s/%s/generated.go", c.generateToDirectory, g.PackagePath))
		utils.FixFieldAlignment(model)
		color.Green.Printf("✅  Generated client: %s 🚀\n", g.RemoteServiceName)
	}
}

// mutateHook adds the "omitempty" option to nilable fields.
// For more info see https://github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/blob/master/docs/content/recipes/modelgen-hook.md
func clientMutateHook(b *modelgen.ModelBuild) *modelgen.ModelBuild {
	for _, model := range b.Models {
		for _, field := range model.Fields {

			omitEmpty := ""
			if genCfg.IsNilable(field.Type) {
				omitEmpty = ",omitempty"
			}

			jsonTag := fmt.Sprintf(`json:"%s%s"`, field.Name, omitEmpty)
			msgpackTag := fmt.Sprintf(`msgpack:"%s%s"`, field.Name, omitEmpty)
			graphql := fmt.Sprintf(`graphql:"%s%s"`, field.Name, omitEmpty)
			tag := fmt.Sprintf(`%s %s %s`, jsonTag, msgpackTag, graphql)
			field.Tag = tag
		}
	}
	return b
}

func generateClientCode(ctx context.Context, g *ClientGenerator, option ...api.Option) error {
	var plugins []plugin.Plugin

	if g.cfg.Model.IsDefined() {
		p := modelgen.Plugin{
			MutateHook: clientMutateHook,
		}
		plugins = append(plugins, &p)
	}
	for _, o := range option {
		o(g.cfg.GQLConfig, &plugins)
	}

	if err := config.LoadSchema(ctx, g.cfg, g.Conn, client.SetRemoteServiceName(g.RemoteServiceName)); err != nil {
		return fmt.Errorf("failed to load schema: %w", err)
	}

	if err := g.cfg.GQLConfig.Init(); err != nil {
		return fmt.Errorf("generating core failed: %w", err)
	}

	for _, p := range plugins {
		if mut, ok := p.(plugin.ConfigMutator); ok {
			err := mut.MutateConfig(g.cfg.GQLConfig)
			if err != nil {
				return fmt.Errorf("%s failed: %w", p.Name(), err)
			}
		}
	}

	return nil
}
