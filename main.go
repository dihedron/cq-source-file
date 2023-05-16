package main

import (
	"github.com/cloudquery/plugin-sdk/serve"
	"github.com/dihedron/cq-source-localfile/plugin"
)

func main() {
	serve.Source(plugin.Plugin())
}
