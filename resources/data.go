package resources

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/dihedron/cq-plugin-utils/format"
	"github.com/dihedron/cq-plugin-utils/pointer"
	"github.com/dihedron/cq-source-file/client"
	"github.com/xuri/excelize/v2"
	"gopkg.in/yaml.v3"
)

func GetTables(ctx context.Context, meta schema.ClientMeta) (schema.Tables, error) {

	client := meta.(*client.Client)

	client.Logger.Debug().Str("file", client.Specs.File).Msg("reading input from file")

	client.Data = []map[string]any{}
	switch strings.ToLower(client.Specs.Format) {
	case "json":
		data, err := os.ReadFile(client.Specs.File)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			return nil, fmt.Errorf("error reading input file %q: %w", client.Specs.File, err)
		}
		client.Logger.Debug().Str("file", client.Specs.File).Msg("input file read")
		if err := json.Unmarshal(data, &client.Data); err != nil {
			client.Logger.Error().Err(err).Msg("error unmarshalling data from JSON")
			return nil, fmt.Errorf("error unmarshalling data from JSON: %w", err)
		}
	case "yaml", "yml":
		data, err := os.ReadFile(client.Specs.File)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			return nil, fmt.Errorf("error reading input file %q: %w", client.Specs.File, err)
		}
		client.Logger.Debug().Str("file", client.Specs.File).Msg("input file read")
		if err := yaml.Unmarshal(data, &client.Data); err != nil {
			client.Logger.Error().Err(err).Msg("error unmarshalling data from JSON")
			return nil, fmt.Errorf("error unmarshalling data from JSON: %w", err)
		}
	case "csv":
		data, err := os.ReadFile(client.Specs.File)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			return nil, fmt.Errorf("error reading input file %q: %w", client.Specs.File, err)
		}
		client.Logger.Debug().Str("file", client.Specs.File).Msg("input file read")
		if client.Specs.Separator == nil {
			client.Specs.Separator = pointer.To(",")
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		var keys []string
		client.Data = []map[string]any{}
		first := true
		for scanner.Scan() {
			line := scanner.Text()
			client.Logger.Debug().Str("line", line).Msg("read line from input file")
			if first {
				first = false
				keys = strings.Split(line, *client.Specs.Separator)
			} else {
				values := strings.Split(line, *client.Specs.Separator)
				entry := map[string]any{}
				for i := 0; i < len(keys); i++ {
					entry[keys[i]] = values[i]
				}
				client.Data = append(client.Data, entry)
			}
		}
	case "xsl", "xlsx", "excel":
		xls, err := excelize.OpenFile(client.Specs.File)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			return nil, fmt.Errorf("error reading input file %q: %w", client.Specs.File, err)
		}
		defer func() {
			if err := xls.Close(); err != nil {
				client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			}
		}()
		// Get all the rows in the Sheet1.
		if client.Specs.Sheet == nil {
			// get the currently active sheet in the file
			client.Specs.Sheet = pointer.To(xls.GetSheetName(xls.GetActiveSheetIndex()))
		}
		client.Logger.Debug().Str("sheet", *client.Specs.Sheet).Msg("getting data from sheet")
		rows, err := xls.GetRows(*client.Specs.Sheet)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error getting rows")
			return nil, fmt.Errorf("error getting rows from input file %q: %w", client.Specs.File, err)
		}

		var keys []string
		client.Data = []map[string]any{}
		first := true
		for _, row := range rows {
			if first {
				first = false
				keys = row
			} else {
				values := row
				entry := map[string]any{}
				for i := 0; i < len(keys); i++ {
					entry[keys[i]] = values[i]
				}
				client.Data = append(client.Data, entry)
			}
		}
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
				Resolver:    fetchColumn,
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
			columns = append(columns, column)
		}
		client.Logger.Debug().Msg("returning table")
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
	return resource.Set(c.Name, item[c.Name])
}
