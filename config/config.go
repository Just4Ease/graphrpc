package config

import (
	"context"
	"fmt"
	"github.com/Just4Ease/axon/v2"
	graphRPClient "github.com/Just4Ease/graphrpc/client"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/99designs/gqlgen/codegen/config"
	//"github.com/Just4Ease/graphrpc/client"
	gencConf "github.com/Yamashou/gqlgenc/config"
	"github.com/Yamashou/gqlgenc/introspection"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/validator"
)

// Config extends the gqlgen basic config
// and represents the config file
type GraphRPCClientConfig gencConf.Config

// StringList is a simple array of strings
type StringList []string

// Has checks if the strings array has a give value
func (a StringList) Has(file string) bool {
	for _, existing := range a {
		if existing == file {
			return true
		}
	}

	return false
}

var path2regex = strings.NewReplacer(
	`.`, `\.`,
	`*`, `.+`,
	`\`, `[\\/]`,
	`/`, `[\\/]`,
)

func LoadClientGeneratorCfg(cfg *GraphRPCClientConfig) (*gencConf.Config, error) {
	var err error

	if cfg.SchemaFilename != nil && cfg.Endpoint != nil {
		return nil, fmt.Errorf("'schema' and 'endpoint' both specified. Use schema to load from a local file, use endpoint to load from a remote server (using introspection)")
	}

	if cfg.SchemaFilename == nil && cfg.Endpoint == nil {
		return nil, fmt.Errorf("neither 'schema' nor 'endpoint' specified. Use schema to load from a local file, use endpoint to load from a remote server (using introspection)")
	}

	// https://github.com/99designs/gqlgen/blob/3a31a752df764738b1f6e99408df3b169d514784/codegen/config/config.go#L120
	for _, f := range cfg.SchemaFilename {
		var matches []string

		// for ** we want to override default globbing patterns and walk all
		// subdirectories to match schema files.
		if strings.Contains(f, "**") {
			pathParts := strings.SplitN(f, "**", 2)
			rest := strings.TrimPrefix(strings.TrimPrefix(pathParts[1], `\`), `/`)
			// turn the rest of the glob into a regex, anchored only at the end because ** allows
			// for any number of dirs in between and walk will let us match against the full path name
			globRe := regexp.MustCompile(path2regex.Replace(rest) + `$`)

			if err := filepath.Walk(pathParts[0], func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if globRe.MatchString(strings.TrimPrefix(path, pathParts[0])) {
					matches = append(matches, path)
				}

				return nil
			}); err != nil {
				return nil, fmt.Errorf("failed to walk schema at root %s: %w", pathParts[0], err)
			}
		} else {
			matches, err = filepath.Glob(f)
			if err != nil {
				return nil, fmt.Errorf("failed to glob schema filename %s: %w", f, err)
			}
		}

		files := StringList{}
		for _, m := range matches {
			if !files.Has(m) {
				files = append(files, m)
			}
		}

		cfg.SchemaFilename = gencConf.StringList(files)
	}

	models := make(config.TypeMap)
	if cfg.Models != nil {
		models = cfg.Models
	}

	sources := make([]*ast.Source, 0)

	for _, filename := range cfg.SchemaFilename {
		filename = filepath.ToSlash(filename)
		var err error
		var schemaRaw []byte
		schemaRaw, err = ioutil.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("unable to open schema: %w", err)
		}

		sources = append(sources, &ast.Source{Name: filename, Input: string(schemaRaw)})
	}

	cfg.GQLConfig = &config.Config{
		Model:  cfg.Model,
		Models: models,
		// TODO: gqlgen must be set exec but client not used
		Exec:       config.ExecConfig{Filename: "generated.go"},
		Directives: map[string]config.DirectiveConfig{},
		Sources:    sources,
	}

	if err := cfg.Client.Check(); err != nil {
		return nil, fmt.Errorf("config.exec: %w", err)
	}

	return (*gencConf.Config)(cfg), nil
}

// LoadSchema load and parses the schema from a local file or a remote server
func LoadSchema(ctx context.Context, c *gencConf.Config, conn axon.EventStore, opts ...graphRPClient.Option) error {
	var schema *ast.Schema

	if c.SchemaFilename != nil {
		s, err := loadLocalSchema(c)
		if err != nil {
			return fmt.Errorf("load local schema failed: %w", err)
		}

		schema = s
	} else {
		s, err := loadRemoteSchema(ctx, c, conn, opts...)
		if err != nil {
			return fmt.Errorf("load remote schema failed: %w", err)
		}

		schema = s
	}

	if schema.Query == nil {
		schema.Query = &ast.Definition{
			Kind: ast.Object,
			Name: "Query",
		}
		schema.Types["Query"] = schema.Query
	}

	c.GQLConfig.Schema = schema

	return nil
}

func loadRemoteSchema(ctx context.Context, c *gencConf.Config, conn axon.EventStore, opts ...graphRPClient.Option) (*ast.Schema, error) {

	if opts == nil {
		opts = make([]graphRPClient.Option, 0)
	}

	if c.Endpoint.Headers == nil {
		c.Endpoint.Headers = make(map[string]string)
	}

	for key, value := range c.Endpoint.Headers {
		opts = append(opts, graphRPClient.SetHeader(key, value))
	}

	opts = append(opts, graphRPClient.SetRemoteGraphQLPath("introspect"))

	rpc, err := graphRPClient.NewClient(conn, opts...)
	if err != nil {
		fmt.Print(err, " Erroj")
		return nil, err
	}

	var res introspection.Query
	if err := rpc.Exec(ctx, "Query", "", &res, nil, nil); err != nil {
		return nil, fmt.Errorf("introspection query failed: %w", err)
	}

	schema, err := validator.ValidateSchemaDocument(introspection.ParseIntrospectionQuery(fmt.Sprintf("graphrpc://%s.introspect", rpc.ServiceName()), res))
	if err != nil && err.Error() != "" {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	return schema, nil
}

func loadLocalSchema(c *gencConf.Config) (*ast.Schema, error) {
	schema, err := gqlparser.LoadSchema(c.GQLConfig.Sources...)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

type GenerateConfig struct {
	Prefix        *NamingConfig `yaml:"prefix,omitempty"`
	Suffix        *NamingConfig `yaml:"suffix,omitempty"`
	UnamedPattern string        `yaml:"unamedPattern,omitempty"`
	Client        *bool         `yaml:"client,omitempty"`
	// if true, used client v2 in generate code
	ClientV2 bool `yaml:"clientV2,omitempty"`
}

func (c *GenerateConfig) ShouldGenerateClient() bool {
	if c == nil {
		return true
	}

	if c.Client != nil && !*c.Client {
		return false
	}

	return true
}

type NamingConfig struct {
	Query    string `yaml:"query,omitempty"`
	Mutation string `yaml:"mutation,omitempty"`
}
