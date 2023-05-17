package plugin

import (
	"github.com/cloudquery/plugin-sdk/plugins/source"
	"github.com/dihedron/cq-source-file/client"
	"github.com/dihedron/cq-source-file/resources"
)

var (
	Version = "development"
)

func Plugin() *source.Plugin {
	return source.NewPlugin(
		"github.com/dihedron-file",
		Version,
		nil, // no static tables
		client.New,
		source.WithDynamicTableOption(resources.GetTables),
	)
}
