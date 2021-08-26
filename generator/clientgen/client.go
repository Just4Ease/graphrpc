package clientgen

import (
	"fmt"
	genCfg "github.com/99designs/gqlgen/codegen/config"
	"github.com/99designs/gqlgen/plugin"
	"github.com/Yamashou/gqlgenc/clientgen"
	gencConf "github.com/Yamashou/gqlgenc/config"
)

var _ plugin.ConfigMutator = &Plugin{}

type Plugin struct {
	queryFilePaths []string
	Client         genCfg.PackageConfig
	GenerateConfig *gencConf.GenerateConfig
	remoteServiceName,
	remoteServiceGraphEntrypoint string
}

func (p *Plugin) Name() string {
	return "clientgen"
}

func New(
	queryFilePaths []string,
	client genCfg.PackageConfig,
	generateConfig *gencConf.GenerateConfig,
	remoteServiceName,
	remoteServiceGraphEntrypoint string,
) *Plugin {
	return &Plugin{
		queryFilePaths:               queryFilePaths,
		Client:                       client,
		GenerateConfig:               generateConfig,
		remoteServiceName:            remoteServiceName,
		remoteServiceGraphEntrypoint: remoteServiceGraphEntrypoint,
	}
}

func (p Plugin) MutateConfig(cfg *genCfg.Config) error {
	querySources, err := clientgen.LoadQuerySources(p.queryFilePaths)
	if err != nil {
		return fmt.Errorf("load query sources failed: %w", err)
	}

	// 1. 全体のqueryDocumentを1度にparse
	// 1. Parse document from source of query

	queryDocument, err := clientgen.ParseQueryDocuments(cfg.Schema, querySources, p.GenerateConfig)
	if err != nil {
		return fmt.Errorf(": %w", err)
	}

	// 2. OperationごとのqueryDocumentを作成
	// 2. Separate documents for each operation
	queryDocuments, err := clientgen.QueryDocumentsByOperations(cfg.Schema, queryDocument.Operations)
	if err != nil {
		return fmt.Errorf("parse query document failed: %w", err)
	}

	// 3. テンプレートと情報ソースを元にコード生成
	// 3. Generate code from template and document source
	sourceGenerator := NewSourceGenerator(cfg, p.Client)

	//d, _ = json.Marshal(p.GenerateConfig)
	//_ = json.Unmarshal(d, &GenerateConfig)
	source := NewSource(cfg.Schema, queryDocument, sourceGenerator, p.GenerateConfig)
	query, err := source.Query()
	if err != nil {
		return fmt.Errorf("generating query object: %w", err)
	}

	mutation, err := source.Mutation()
	if err != nil {
		return fmt.Errorf("generating mutation object: %w", err)
	}

	fragments, err := source.Fragments()
	if err != nil {
		return fmt.Errorf("generating fragment failed: %w", err)
	}

	operationResponses, err := source.OperationResponses()
	if err != nil {
		return fmt.Errorf("generating operation response failed: %w", err)
	}

	operations, err := source.Operations(queryDocuments)
	if err != nil {
		return fmt.Errorf("generating operation failed: %w", err)
	}

	generateClient := p.GenerateConfig.ShouldGenerateClient()


	if err := RenderTemplate(cfg,
		query,
		mutation,
		fragments,
		operations,
		operationResponses,
		generateClient,
		p.Client,
		p.remoteServiceName,
		p.remoteServiceGraphEntrypoint,
	); err != nil {
		return fmt.Errorf("template failed: %w", err)
	}

	return nil
}
