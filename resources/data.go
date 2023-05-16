package resources

import (
	"context"
	"errors"

	"github.com/cloudquery/plugin-sdk/schema"
)

func GetTables(ctx context.Context, c schema.ClientMeta) (schema.Tables, error) {
	return nil, errors.New("not implemented")
}

/*
func Data() *schema.Table {
	return &schema.Table{
		Name:     "openstack_volumes",
		Resolver: fetchData,
		Transform: transformers.TransformWithStruct(
			&map[string]any{},
			transformers.WithPrimaryKeys("ID"),
			transformers.WithNameTransformer(transform.TagNameTransformer), // use cq-name tags to translate name
			transformers.WithTypeTransformer(transform.TagTypeTransformer), // use cq-type tags to translate type
			transformers.WithSkipFields("Links"),
		),
		// Columns: []schema.Column{
		// 	{
		// 		Name:        "tags",
		// 		Type:        schema.TypeStringArray,
		// 		Description: "The set of tags on the project.",
		// 		Resolver: transform.Apply(
		// 			transform.OnObjectField("Tags"),
		// 		),
		// 	},
		// },
	}
}

func fetchData(ctx context.Context, meta schema.ClientMeta, parent *schema.Resource, res chan<- interface{}) error {

	//meta.(*client.Client)

	return nil
}
*/
