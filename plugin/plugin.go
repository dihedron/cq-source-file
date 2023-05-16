package plugin

import (
	"github.com/cloudquery/plugin-sdk/plugins/source"
	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/dihedron/cq-source-localfile/client"
	"github.com/dihedron/cq-source-localfile/resources"
)

var (
	Version = "development"
)

func Plugin() *source.Plugin {
	return source.NewPlugin(
		"github.com/dihedron-localfile",
		Version,
		schema.Tables{},
		client.New,
		source.WithDynamicTableOption(resources.GetTables),
	)
}
