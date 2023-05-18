package client

type Column struct {
	Name        string  `json:"name,omitempty" yaml:"name,omitempty"`
	Description *string `json:"description,omitempty" yaml:"description,omitempty"`
	Type        string  `json:"type,omitempty" yaml:"type,omitempty"`
	Key         bool    `json:"key,omitempty" yaml:"pk,omitempty"`
	Unique      bool    `json:"unique,omitempty" yaml:"unique,omitempty"`
	NotNull     bool    `json:"notnull,omitempty" yaml:"notnull,omitempty"`
}
type Spec struct {
	File      string   `json:"file,omitempty" yaml:"file,omitempty"`
	Format    string   `json:"format,omitempty" yaml:"format,omitempty"`
	Table     string   `json:"table,omitempty" yaml:"table,omitempty"`
	Columns   []Column `json:"columns,omitempty" yaml:"columns,omitempty"`
	Separator *string  `json:"separator,omitempty" yaml:"separator,omitempty"` // CSV only
	Sheet     *string  `json:"sheet,omitempty" yaml:"sheet,omitempty"`         // XLSX only
}
