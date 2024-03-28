package resources

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/dihedron/cq-plugin-utils/format"
	"github.com/dihedron/cq-plugin-utils/pointer"
	"github.com/dihedron/cq-source-file/client"
	"github.com/expr-lang/expr"
	"github.com/rs/zerolog"
)

type Env struct {
	Row map[string]any `expr:"row"`
}

// GetTable uses data in the spec section of the client configuration to
// dynamically build the information about the columns being imported.
func GetDynamicTables(logger zerolog.Logger, spec *client.Spec) schema.Tables {

	// get the table columns and populate the admission filter
	// for the main table
	tableColumns, err := buildTableColumnsSchema(logger, &spec.Table)
	if err != nil {
		logger.Error().Err(err).Str("table", spec.Table.Name).Msg("error getting table column schema")
		return nil
	}

	// now loop over and add relations
	relations := []*schema.Table{}
	logger.Debug().Str("table", spec.Table.Name).Msg("adding relations...")
	for _, relation := range spec.Relations {

		relationColumns, err := buildTableColumnsSchema(logger, &relation)
		if err != nil {
			logger.Error().Err(err).Str("table", relation.Name).Msg("error getting relation column schema")
			return nil
		}

		logger.Debug().Str("relation", relation.Name).Msg("adding relation to schema")

		if relation.Description == nil {
			relation.Description = pointer.To(fmt.Sprintf("Table %q", relation.Name))
		}

		relations = append(relations, &schema.Table{
			Name:        relation.Name,
			Description: *relation.Description,
			Resolver:    fetchRelationData(&relation),
			Columns:     relationColumns,
		})
	}

	// now put the main table with its relations together
	logger.Debug().Msg("returning table schema")
	if spec.Table.Description == nil {
		spec.Table.Description = pointer.To(fmt.Sprintf("Table %q", spec.Table.Name))
	}

	return []*schema.Table{
		{
			Name:        spec.Table.Name,
			Description: *spec.Table.Description,
			Resolver:    fetchTableData(&spec.Table),
			Columns:     tableColumns,
			Relations:   relations,
		},
	}
}

// buildTableColumnsSchema returns the schema definition of the given table's columns
// and populates the table's Evaluator field if the Filter is not null (side effect).
// TODO: fix side effect once working
func buildTableColumnsSchema(logger zerolog.Logger, table *client.Table) ([]schema.Column, error) {
	row := map[string]any{}

	// start by looping over the columns definitions and creating the Column schema
	// object; while looping over the columns, we are also creating a map holding
	// the column names and an example (zero) value for each column, which we'll use
	// when initialising the admission filter, which expects to work on a given data
	// structure when being compiled.
	columns := []schema.Column{}

	for _, c := range table.Columns {
		logger.Debug().Str("table", table.Name).Str("name", c.Name).Msg("adding column")

		// prepare the template for value transformation if there is a transform
		if c.Transform != nil {
			tpl, err := template.New(c.Name).Funcs(sprig.FuncMap()).Parse(*c.Transform)
			if err != nil {
				logger.Error().Err(err).Str("table", table.Name).Str("column", c.Name).Str("transform", *c.Transform).Msg("error parsing column transform")
				return nil, fmt.Errorf("error parsing transform for column %q: %w", c.Name, err)
			} else {
				c.Template = tpl
				logger.Debug().Str("table", table.Name).Str("template", format.ToJSON(tpl)).Str("transform", *c.Transform).Msg("template after having parsed transform")
			}
			logger.Debug().Str("table", table.Name).Str("column", c.Name).Str("specs", format.ToJSON(table.Columns)).Msg("column metadata after having parsed transform")
		}

		if c.Description == nil {
			c.Description = pointer.To(fmt.Sprintf("The column mapping the %q field from the input data", c.Name))
		}
		column := schema.Column{
			Name:        c.Name,
			Description: *c.Description,
			Resolver:    fetchColumn(table),
			PrimaryKey:  c.Key,
			Unique:      c.Unique,
			NotNull:     c.NotNull,
		}
		switch strings.ToLower(c.Type) {
		case "string", "str", "s":
			logger.Debug().Str("table", table.Name).Str("name", c.Name).Msg("column is of type string")
			column.Type = arrow.BinaryTypes.String
			row[c.Name] = ""
		case "integer", "int", "i":
			logger.Debug().Str("table", table.Name).Str("name", c.Name).Msg("column is of type int")
			column.Type = arrow.PrimitiveTypes.Int64
			row[c.Name] = 0
		case "boolean", "bool", "b":
			logger.Debug().Str("table", table.Name).Str("name", c.Name).Msg("column is of type bool")
			column.Type = arrow.FixedWidthTypes.Boolean
			row[c.Name] = false
		default:
			logger.Debug().Str("table", table.Name).Str("name", c.Name).Msg("column is of unmapped type, assuming string")
			column.Type = arrow.BinaryTypes.String
			row[c.Name] = ""
		}
		columns = append(columns, column)
	}

	// now initialise the filter using the row map that we've populated above;
	// TODO: note that this function has the side effect of populating the table
	// Program field with the admission filter expression evaluator; this is
	// not a good practice but we'll fix it once this implementation is working
	if table.Filter != nil {
		logger.Debug().Str("table", table.Name).Str("filter", *table.Filter).Str("row template", format.ToJSON(row)).Msg("compiling row filter")
		env := map[string]any{
			"_": row,
			"string": func(v any) string {
				return fmt.Sprintf("%v", v)
			},
		}
		if program, err := expr.Compile(*table.Filter, expr.Env(env), expr.AsBool()); err != nil {
			logger.Error().Err(err).Str("table", table.Name).Str("filter", *table.Filter).Msg("error compiling expression evaluator")
		} else {
			logger.Debug().Str("table", table.Name).Str("filter", *table.Filter).Msg("expression evaluator successfully compiled")
			table.Evaluator = program
		}
	}

	logger.Debug().Str("table", table.Name).Str("columns", format.ToJSON(columns)).Msg("returning columns schema")
	return columns, nil
}
