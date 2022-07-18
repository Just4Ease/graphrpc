package generator

// Original work from https://github.com/Yamashou/gqlgenc/blob/1ef8055d/generator/generater.go

import (
	"context"
	"fmt"
	"github.com/infiotinc/gqlgenc/config"

	"github.com/99designs/gqlgen/api"
	"github.com/99designs/gqlgen/plugin"
)

func Generate(ctx context.Context, cfg *config.Config, option ...api.Option) error {
	var plugins []plugin.Plugin
	for _, o := range option {
		o(cfg.GQLConfig, &plugins)
	}

	if err := cfg.LoadSchema(ctx); err != nil {
		return fmt.Errorf("failed to load schema: %w", err)
	}

	if err := cfg.GQLConfig.Init(); err != nil {
		return fmt.Errorf("generating core failed: %w", err)
	}

	for _, p := range plugins {
		if mut, ok := p.(plugin.ConfigMutator); ok {
			err := mut.MutateConfig(cfg.GQLConfig)
			if err != nil {
				return fmt.Errorf("%s failed: %w", p.Name(), err)
			}
		}
	}

	return nil
}
