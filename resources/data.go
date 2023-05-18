package resources

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/dihedron/cq-plugin-utils/format"
	"github.com/dihedron/cq-plugin-utils/pointer"
	"github.com/dihedron/cq-source-file/client"
	"github.com/xuri/excelize/v2"
	"gopkg.in/yaml.v3"
)

// GetTable uses data in the spec section of the client configuration to
// dynamically build the information about the columns being imported.
func GetTables(ctx context.Context, meta schema.ClientMeta) (schema.Tables, error) {
	client := meta.(*client.Client)

	columns := []schema.Column{}
	for _, c := range client.Specs.Columns {
		client.Logger.Debug().Str("name", c.Name).Msg("adding column")
		if c.Description == nil {
			c.Description = pointer.To(fmt.Sprintf("The column mapping the %q field from the input data", c.Name))
		}
		column := schema.Column{
			Name:        c.Name,
			Description: *c.Description,
			Resolver:    fetchColumn,
			CreationOptions: schema.ColumnCreationOptions{
				PrimaryKey: c.Key,
				Unique:     c.Unique,
				NotNull:    c.NotNull,
			},
		}
		switch strings.ToLower(c.Type) {
		case "string", "str", "s":
			client.Logger.Debug().Str("name", c.Name).Msg("column is of type string")
			column.Type = schema.TypeString
		case "integer", "int", "i":
			client.Logger.Debug().Str("name", c.Name).Msg("column is of type int")
			column.Type = schema.TypeInt
		case "boolean", "bool", "b":
			client.Logger.Debug().Str("name", c.Name).Msg("column is of type bool")
			column.Type = schema.TypeBool
		default:
			client.Logger.Debug().Str("name", c.Name).Msg("column is of unmapped type, assuming string")
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

// fetchData reads the input file and unmarshals it into a set of rows using
// format-specific mechanisms, then encodes the information as a map[string]any
// per row and returns it; fetchColumn knows how to pick the data out of this
// map and set it into the resource being returned to ClouqQuery.
func fetchData(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {
	client := meta.(*client.Client)

	rows := []map[string]any{}
	client.Logger.Debug().Msg("fetching data...")
	switch strings.ToLower(client.Specs.Format) {
	case "json":
		data, err := os.ReadFile(client.Specs.File)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			return fmt.Errorf("error reading input file %q: %w", client.Specs.File, err)
		}
		client.Logger.Debug().Str("file", client.Specs.File).Msg("input file read")
		if err := json.Unmarshal(data, &rows); err != nil {
			client.Logger.Error().Err(err).Msg("error unmarshalling data from JSON")
			return fmt.Errorf("error unmarshalling data from JSON: %w", err)
		}
	case "yaml", "yml":
		data, err := os.ReadFile(client.Specs.File)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			return fmt.Errorf("error reading input file %q: %w", client.Specs.File, err)
		}
		client.Logger.Debug().Str("file", client.Specs.File).Msg("input file read")
		if err := yaml.Unmarshal(data, &rows); err != nil {
			client.Logger.Error().Err(err).Msg("error unmarshalling data from YAML")
			return fmt.Errorf("error unmarshalling data from YAML: %w", err)
		}
	case "csv":
		data, err := os.ReadFile(client.Specs.File)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			return fmt.Errorf("error reading input file %q: %w", client.Specs.File, err)
		}
		client.Logger.Debug().Str("file", client.Specs.File).Msg("input file read")
		if client.Specs.Separator == nil {
			client.Specs.Separator = pointer.To(",")
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		first := true
		var keys []string
		for scanner.Scan() {
			line := scanner.Text()
			client.Logger.Debug().Str("line", line).Msg("read line from input file")
			if first {
				first = false
				keys = strings.Split(line, *client.Specs.Separator)
			} else {
				values := strings.Split(line, *client.Specs.Separator)
				if len(values) >= len(keys) {
					row := map[string]any{}
					for i := 0; i < len(keys); i++ {
						for _, column := range client.Specs.Columns {

							if keys[i] == column.Name {
								row[client.Specs.Columns[i].Name] = values[i]
							}
						}
					}
					rows = append(rows, row)
				} else {
					client.Logger.Warn().Str("file", client.Specs.File).Str("line", line).Int("expected", len(keys)).Int("actual", len(values)).Msg("invalid number of columns")
				}
			}
		}
	case "xsl", "xlsx", "excel":
		xls, err := excelize.OpenFile(client.Specs.File)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			return fmt.Errorf("error reading input file %q: %w", client.Specs.File, err)
		}
		defer func() {
			if err := xls.Close(); err != nil {
				client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error reading input file")
			}
		}()
		// get all the rows in the requested (or the active) sheet
		if client.Specs.Sheet == nil {
			// get the currently active sheet in the file
			client.Specs.Sheet = pointer.To(xls.GetSheetName(xls.GetActiveSheetIndex()))
		}
		client.Logger.Debug().Str("sheet", *client.Specs.Sheet).Msg("getting data from sheet")
		xlsrows, err := xls.GetRows(*client.Specs.Sheet)
		if err != nil {
			client.Logger.Error().Err(err).Str("file", client.Specs.File).Msg("error getting rows")
			return fmt.Errorf("error getting rows from input file %q: %w", client.Specs.File, err)
		}

		var keys []string
		first := true
		for _, xlsrow := range xlsrows {
			if first {
				first = false
				keys = xlsrow
			} else {
				values := xlsrow
				row := map[string]any{}
				for i := 0; i < len(keys); i++ {
					if i < len(values) {
						// XLSX rows can be sparse, in which case all TRAILING empty cells are removed
						// from the returned slice; empty cells in the middle are still valid
						row[keys[i]] = values[i]
					} else {
						row[keys[i]] = nil
					}
				}
				rows = append(rows, row)
			}
		}

		// TODO: add more formats
	default:
		client.Logger.Error().Str("format", client.Specs.Format).Msg("unsupported format")
		return fmt.Errorf("unsupported format: %q", client.Specs.Format)

	}

	for _, row := range rows {
		client.Logger.Debug().Str("row", format.ToJSON(row)).Msg("returning single row")
		res <- row
	}
	return nil
}

// fetchColumn picks the value under the right key from the map[string]any
// and sets it into the resource being returned to CloudQuery.
func fetchColumn(ctx context.Context, meta schema.ClientMeta, resource *schema.Resource, c schema.Column) error {
	client := meta.(*client.Client)
	// client.Logger.Debug().Str("resource", format.ToJSON(resource)).Str("column", format.ToJSON(c)).Str("item type", fmt.Sprintf("%T", resource.Item)).Msg("fetching column...")
	item := resource.Item.(map[string]any)
	value := item[c.Name]
	client.Logger.Debug().Str("value", fmt.Sprintf("%v", value)).Str("type", fmt.Sprintf("%T", value)).Msg("checking value type")
	if value == nil {
		client.Logger.Warn().Msg("value is nil")
		if c.CreationOptions.NotNull {
			err := fmt.Errorf("invalid nil value for non-nullable column %s", c.Name)
			client.Logger.Error().Err(err).Str("name", c.Name).Msg("error setting column")
			return err
		}
	} else {
		client.Logger.Warn().Msg("value is NOT nil")
		if reflect.ValueOf(value).IsZero() {
			if !c.CreationOptions.NotNull {
				// column is nullable, let's null it
				client.Logger.Warn().Str("name", c.Name).Msg("nulling column value")
				value = nil
			} else {
				client.Logger.Warn().Msg("set default value for type")
				switch c.Type {
				case schema.TypeBool:
					value = false
				case schema.TypeInt:
					value = 0
				case schema.TypeString:
					value = ""
				}
			}
		}
	}
	// in XLSX some values may be null, in which case we must
	// be sure we're not asking cloudQuery to parse invalid values
	return resource.Set(c.Name, value)
}
