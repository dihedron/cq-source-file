package client

type Spec struct {
	File   string            `json:"file,omitempty" yaml:"file,omitempty"`
	Format string            `json:"format,omitempty" yaml:"format,omitempty"`
	Table  string            `json:"table,omitempty" yaml:"table,omitempty"`
	Keys   []string          `json:"keys,omitempty" yaml:"keys,omitempty"`
	Types  map[string]string `json:"types,omitempty" yaml:"types,omitempty"`
}
