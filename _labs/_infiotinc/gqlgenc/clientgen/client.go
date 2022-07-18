package clientgen

import (
	"fmt"
	gqlgenCfg "github.com/99designs/gqlgen/codegen/config"
	"github.com/99designs/gqlgen/plugin"
	"github.com/infiotinc/gqlgenc/config"
)

var _ plugin.ConfigMutator = &Plugin{}

type Plugin struct {
	queryFilePaths []string
	Client         gqlgenCfg.PackageConfig
	GenerateConfig *config.GenerateConfig
	Cfg            *config.Config
}

func New(cfg *config.Config) *Plugin {
	return &Plugin{
		queryFilePaths: cfg.Query,
		Client:         cfg.Client.PackageConfig,
		GenerateConfig: cfg.Generate,
		Cfg:            cfg,
	}
}

func (p *Plugin) Name() string {
	return "clientgen"
}

func (p *Plugin) MutateConfig(cfg *gqlgenCfg.Config) error {
	querySources, err := LoadQuerySources(p.queryFilePaths)
	if err != nil {
		return fmt.Errorf("load query sources failed: %w", err)
	}

	// 1. 全体のqueryDocumentを1度にparse
	// 1. Parse document from source of query
	queryDocument, err := ParseQueryDocuments(cfg.Schema, querySources)
	if err != nil {
		return fmt.Errorf(": %w", err)
	}

	// 2. OperationごとのqueryDocumentを作成
	// 2. Separate documents for each operation
	queryDocuments, err := QueryDocumentsByOperations(cfg.Schema, queryDocument.Operations)
	if err != nil {
		return fmt.Errorf("parse query document failed: %w", err)
	}

	// 3. テンプレートと情報ソースを元にコード生成
	// 3. Generate code from template and document source
	sourceGenerator := NewSourceGenerator(cfg, p.Cfg, p.Client)
	source := NewSource(cfg.Schema, queryDocument, sourceGenerator, p.GenerateConfig)

	err = source.ExtraTypes()
	if err != nil {
		return fmt.Errorf("generating extra types failed: %w", err)
	}

	err = source.Fragments()
	if err != nil {
		return fmt.Errorf("generating fragment failed: %w", err)
	}

	operationResponses, err := source.OperationResponses()
	if err != nil {
		return fmt.Errorf("generating operation response failed: %w", err)
	}

	operations := source.Operations(queryDocuments, operationResponses)

	genTypes := sourceGenerator.GenTypes()
	ptrTypes := sourceGenerator.PtrTypes()

	generateClient := p.GenerateConfig.ShouldGenerateClient()
	if err := RenderTemplate(cfg, genTypes, ptrTypes, operations, generateClient, p.Client); err != nil {
		return fmt.Errorf("template failed: %w", err)
	}

	return nil
}
