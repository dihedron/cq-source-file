package client

import (
	"context"
	"fmt"

	"github.com/cloudquery/plugin-sdk/plugins/source"
	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/cloudquery/plugin-sdk/specs"
	"github.com/dihedron/cq-plugin-utils/format"
	"github.com/rs/zerolog"
)

type Client struct {
	Logger zerolog.Logger
	Specs  *Spec
	//Data   []map[string]any
}

func (c *Client) ID() string {
	return "github.com/dihedron/cq-source-localfile"
}

func New(ctx context.Context, logger zerolog.Logger, s specs.Source, opts source.Options) (schema.ClientMeta, error) {
	var pluginSpec Spec

	if err := s.UnmarshalSpec(&pluginSpec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal plugin spec: %w", err)
	}

	logger.Debug().Str("spec", format.ToJSON(pluginSpec)).Msg("plugin spec")

	return &Client{
		Logger: logger,
		Specs:  &pluginSpec,
	}, nil
}
