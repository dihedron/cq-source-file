package client

type Spec struct {
	Path  *string `json:"path,omitempty" yaml:"path,omitempty"`
	Table *string `json:"table,omitempty" yaml:"table,omitempty"`
	// Header *bool   `json:"header,omitempty" yaml:"header,omitempty"`
}
