package main

import (
	"github.com/cloudquery/plugin-sdk/serve"
	"github.com/dihedron/cq-source-file/plugin"
)

func main() {
	p := plugin.Plugin()
	serve.Source(p)
}
