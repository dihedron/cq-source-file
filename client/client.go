package client

import (
	"context"
	"fmt"

	"github.com/dihedron/cq-plugin-utils/format"
	"github.com/rs/zerolog"
)

type Client struct {
	Specs Spec

	logger zerolog.Logger
}

func (c *Client) ID() string {
	return "github.com/dihedron/cq-source-file"
}

func (c *Client) Logger() *zerolog.Logger {
	return &c.logger
}

func New(ctx context.Context, logger zerolog.Logger, spec *Spec) (*Client, error) {

	logger.Debug().Str("specs", format.ToJSON(spec)).Msg("plugin configuration")

	err := spec.Validate()
	if err != nil {
		logger.Error().Err(err).Msg("invalid spec configuration")
		return nil, fmt.Errorf("error spec not valid: %w", err)
	}

	return &Client{
		Specs:  *spec,
		logger: logger,
	}, nil
}
