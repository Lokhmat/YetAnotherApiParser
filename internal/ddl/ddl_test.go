package ddl

import (
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestGenerateDDLFromObjectSchema_WithRelationColumns(t *testing.T) {
	schema := openapi3.NewObjectSchema().
		WithProperty("id", withPK(openapi3.NewIntegerSchema())).
		WithProperty("name", openapi3.NewStringSchema()).
		WithProperty("child", openapi3.NewObjectSchema().
			WithProperty("cid", openapi3.NewIntegerSchema()))
	schema.Required = []string{"id", "name"}

	sql, err := GenerateDDLFromObjectSchema(schema, "users", []RelationColumnSpec{
		{Name: "child", Type: "INTEGER"},
		{Name: "phones", Type: "INTEGER[]"},
	})
	if err != nil {
		t.Fatalf("GenerateDDLFromObjectSchema returned error: %v", err)
	}

	if !strings.Contains(sql, "CREATE TABLE users") {
		t.Fatalf("unexpected SQL: %s", sql)
	}
	if !strings.Contains(sql, "id INTEGER PRIMARY KEY") {
		t.Fatalf("expected PK column: %s", sql)
	}
	if !strings.Contains(sql, "name TEXT NOT NULL") {
		t.Fatalf("expected scalar not-null column: %s", sql)
	}
	if !strings.Contains(sql, "child INTEGER") {
		t.Fatalf("expected direct-ref column: %s", sql)
	}
	if !strings.Contains(sql, "phones INTEGER[]") {
		t.Fatalf("expected array-ref column: %s", sql)
	}
}

func TestGenerateJoinTableDDL(t *testing.T) {
	sql := GenerateJoinTableDDL("a_b_link", "a_id", "INTEGER", "b_id", "BIGINT")
	if !strings.Contains(sql, "CREATE TABLE a_b_link") {
		t.Fatalf("unexpected SQL: %s", sql)
	}
	if !strings.Contains(sql, "a_id INTEGER NOT NULL") {
		t.Fatalf("unexpected SQL: %s", sql)
	}
	if !strings.Contains(sql, "b_id BIGINT NOT NULL") {
		t.Fatalf("unexpected SQL: %s", sql)
	}
	if !strings.Contains(sql, "PRIMARY KEY (a_id, b_id)") {
		t.Fatalf("unexpected SQL: %s", sql)
	}
}

func withPK(schema *openapi3.Schema) *openapi3.Schema {
	if schema.Extensions == nil {
		schema.Extensions = map[string]any{}
	}
	schema.Extensions["x-pk"] = true
	return schema
}
