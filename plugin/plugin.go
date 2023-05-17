package plugin

import (
	"github.com/cloudquery/plugin-sdk/plugins/source"
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
		nil, // no static tables
		client.New,
		source.WithDynamicTableOption(resources.GetTables),
	)
}
