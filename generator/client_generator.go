package generator

import (
	"context"
	"fmt"
	"github.com/99designs/gqlgen/api"
	genCfg "github.com/99designs/gqlgen/codegen/config"
	"github.com/99designs/gqlgen/plugin"
	"github.com/99designs/gqlgen/plugin/modelgen"
	"github.com/Just4Ease/graphrpc/client"
	"github.com/Just4Ease/graphrpc/config"
	"github.com/Just4Ease/graphrpc/generator/clientgen"
	gencConf "github.com/Yamashou/gqlgenc/config"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"os"
	"path"
	"strings"
)

type ClientGenerator struct {
	PackageName          string
	PackagePath          string
	RemoteServiceName          string
	RemoteServiceGraphEntrypoint    string
	customTypes          []customType
	QueriesPath          string
	QueryParamsPrefix    string
	QueryParamsSuffix    string
	MutationParamsPrefix string
	MutationParamsSuffix string
	Headers              map[string]string
	ClientV2             bool
	cfg                  *gencConf.Config
	NatsConn             *nats.Conn
	NatsConnOpts         []nats.Option
	NatsConnUrl          string
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

// UseNatsConnection is an Option to set nats connection to be used for introspection on remote GraphRPC server
// This overrides SetNatsOptions & SetNatsConnectionURL Options
func UseNatsConnection(nc *nats.Conn) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		if nc == nil {
			return errors.New("invalid nats connection provided")
		}
		o.NatsConn = nc
		return nil
	}
}

// SetNatsOptions is an Option to set internal nats client options that'll be used to connect to remote GraphRPC server for introspection
func SetNatsOptions(option ...nats.Option) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		o.NatsConnOpts = append(o.NatsConnOpts, option...)
		return nil
	}
}

// SetNatsConnectionURL is an Option to set internal nats client connection url that'll be used to connect to remote GraphRPC server for introspection
func SetNatsConnectionURL(url string) ClientGeneratorOption {
	return func(o *ClientGenerator) error {
		o.NatsConnUrl = url
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

func (c *Clients) AddClient(RemoteServiceName string, opts ...ClientGeneratorOption) error {
	clientGenerator := &ClientGenerator{
		RemoteServiceName: RemoteServiceName,
	}

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

	model := path.Clean(fmt.Sprintf("%s/%s/models.go", c.generateToDirectory, clientGenerator.PackagePath))
	generated := path.Clean(fmt.Sprintf("%s/%s/generated.go", c.generateToDirectory, clientGenerator.PackagePath))

	cfgParams := &config.GraphRPCClientConfig{
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
		Endpoint: &gencConf.EndPointConfig{
			URL:     clientGenerator.RemoteServiceGraphEntrypoint,
			Headers: clientGenerator.Headers,
		},
		Generate: nil,
		Query:    query,
	}

	if clientGenerator.ClientV2 {
		cfgParams.Generate = &gencConf.GenerateConfig{
			Prefix: &gencConf.NamingConfig{
				Query:    clientGenerator.QueryParamsPrefix,
				Mutation: clientGenerator.MutationParamsPrefix,
			},
			Suffix: &gencConf.NamingConfig{
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
		//if g.cfg.Generate != nil {
		//	if g.cfg.Generate.ClientV2 {
		//
		//	}
		//}

		//clientGen := api.AddPlugin(clientgenv2.New(g.cfg.Query, g.cfg.Client, g.cfg.Generate))

		if err := generateClientCode(ctx, g, clientGen); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%+v", err.Error())
			os.Exit(4)
		}
	}
}

// mutateHook adds the "omitempty" option to nilable fields.
// For more info see https://github.com/99designs/gqlgen/blob/master/docs/content/recipes/modelgen-hook.md
func clientMutateHook(b *modelgen.ModelBuild) *modelgen.ModelBuild {
	for _, model := range b.Models {
		for _, field := range model.Fields {
			field.Tag = `json:"` + field.Name
			if genCfg.IsNilable(field.Type) {
				field.Tag += ",omitempty"
			}
			field.Tag += `"`
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

	remoteSchemaOpts := make([]client.Option, 0)
	if g.NatsConnUrl != "" {
		remoteSchemaOpts = append(remoteSchemaOpts, client.SetNatsUrl(g.NatsConnUrl))
	}

	if g.NatsConn != nil {
		remoteSchemaOpts = append(remoteSchemaOpts, client.UseNatsCon(g.NatsConn))
	}

	if g.NatsConnOpts != nil {
		remoteSchemaOpts = append(remoteSchemaOpts, client.SetNatsOptions(g.NatsConnOpts...))
	}

	if err := config.LoadSchema(ctx, g.cfg, g.RemoteServiceName, remoteSchemaOpts...); err != nil {
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
