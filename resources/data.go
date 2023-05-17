package resources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/dihedron/cq-plugin-utils/format"
	"github.com/dihedron/cq-source-localfile/client"
	"gopkg.in/yaml.v3"
)

func GetTables(ctx context.Context, meta schema.ClientMeta) (schema.Tables, error) {

	client := meta.(*client.Client)

	client.Logger.Debug().Str("file", client.Specs.File).Msg("reading input from file")

	data, err := os.ReadFile(client.Specs.File)
	if err != nil {
		client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
		return nil, fmt.Errorf("error reading input file %q: %w", client.Specs.File, err)
	}

	client.Logger.Debug().Str("file", client.Specs.File).Msg("input file read")

	client.Data = []map[string]any{}
	switch strings.ToLower(client.Specs.Format) {
	case "json":
		if err := json.Unmarshal(data, &client.Data); err != nil {
			client.Logger.Error().Err(err).Msg("error unmarshalling data from JSON")
			return nil, fmt.Errorf("error unmarshalling data from JSON: %w", err)
		}
	case "yaml":
		if err := yaml.Unmarshal(data, &client.Data); err != nil {
			client.Logger.Error().Err(err).Msg("error unmarshalling data from JSON")
			return nil, fmt.Errorf("error unmarshalling data from JSON: %w", err)
		}
		// TODO: add eXcel, TOML, CSV
	default:
		client.Logger.Error().Str("format", client.Specs.Format).Msg("unsupported format")
		return nil, fmt.Errorf("unsupported format: %q", client.Specs.Format)
	}

	if len(client.Data) > 0 {
		columns := []schema.Column{}
		for name := range client.Data[0] {

			client.Logger.Debug().Str("name", name).Msg("adding column")
			column := schema.Column{
				Name:        name,
				Description: fmt.Sprintf("The column mapping the %q field from the input data", name),
			}

			for _, v := range client.Specs.Keys {
				if name == v {
					client.Logger.Debug().Str("name", name).Msg("column is primary key")
					column.CreationOptions.PrimaryKey = true
					break
				}
			}

			switch strings.ToLower(client.Specs.Types[name]) {
			case "string", "str", "s":
				client.Logger.Debug().Str("name", name).Msg("column is of type string")
				column.Type = schema.TypeString
			case "integer", "int", "i":
				client.Logger.Debug().Str("name", name).Msg("column is of type int")
				column.Type = schema.TypeInt
			case "boolean", "bool", "b":
				client.Logger.Debug().Str("name", name).Msg("column is of type bool")
				column.Type = schema.TypeBool
			default:
				client.Logger.Debug().Str("name", name).Msg("column is of unmapped type, assuming string")
				column.Type = schema.TypeString
			}
			column.Resolver = fetchColumn

			columns = append(columns, column)
		}

		client.Logger.Debug().Msg("returning tables")
		return []*schema.Table{
			{
				Name:     client.Specs.Table,
				Resolver: fetchData,
				Columns:  columns,
			},
		}, nil
	}

	return nil, errors.New("no data in file")
}

func fetchData(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {

	client := meta.(*client.Client)

	client.Logger.Debug().Msg("fetching data...")

	for _, row := range client.Data {
		client.Logger.Debug().Msg("returning single row")
		res <- row
	}
	return nil
}

func fetchColumn(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource, c schema.Column) error {
	client := meta.(*client.Client)

	client.Logger.Debug().Str("resource", format.ToJSON(resource)).Str("column", format.ToJSON(c)).Str("item type", fmt.Sprintf("%T", resource.Item)).Msg("fetching column...")
	item := resource.Item.(map[string]any)
	return resource.Set(c.Name, item[c.Name]) //"aaa")//funk.Get(r.Item, path, funk.WithAllowZero()))
	// for k, v := range item {
	// 	client.Logger.Debug().Str("key", k).Any("value", v).Msg("map entry")
	// }

	// return nil
}
