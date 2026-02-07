package ddl

import (
	"fmt"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
)

type Table struct {
	Name    string
	Columns []Column
	PrimaryKey string
}

type Column struct {
	Name     string
	Type     string
	Nullable bool
	PrimaryKey bool
}

func GenerateDDLFromSchema(schemaRef *openapi3.SchemaRef, spec *openapi3.T, schemaName string) (string, error) {
	if schemaRef == nil || schemaRef.Value == nil {
		return "", fmt.Errorf("schema is nil")
	}

	schema := schemaRef.Value

	// Handle array type
	if schema.Type != nil && schema.Type.Is("array") && schema.Items != nil {
		return GenerateDDLFromSchema(schema.Items, spec, schemaName)
	}

	// Handle object type
	if schema.Type != nil && schema.Type.Is("object") && schema.Properties != nil {
		table := Table{
			Name:    schemaName,
			Columns: []Column{},
		}

		for propName, propRef := range schema.Properties {
			if propRef == nil || propRef.Value == nil {
				continue
			}

			prop := propRef.Value
			colType := goTypeToSQLType(prop.Type, prop.Format)
			nullable := !contains(schema.Required, propName)

			// Check for x-pk extension
			if ext, ok := prop.Extensions["x-pk"].(bool); ok && ext {
				table.PrimaryKey = propName
			}

			table.Columns = append(table.Columns, Column{
				Name:       propName,
				Type:       colType,
				Nullable:   nullable,
				PrimaryKey: table.PrimaryKey == propName,
			})
		}

		return generateCreateTableSQL(table), nil
	}

	return "", fmt.Errorf("unsupported schema type: %v", schema.Type)
}

func generateCreateTableSQL(table Table) string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", table.Name))

	for i, col := range table.Columns {
		if i > 0 {
			b.WriteString(",\n")
		}

		b.WriteString(fmt.Sprintf("  %s %s", col.Name, col.Type))

		if col.PrimaryKey {
			b.WriteString(" PRIMARY KEY")
		} else if !col.Nullable {
			b.WriteString(" NOT NULL")
		}
	}

	b.WriteString("\n);")
	return b.String()
}

func goTypeToSQLType(t *openapi3.Types, format string) string {
	if t == nil {
		return "TEXT"
	}

	// Use the Is method to check type
	if t.Is("integer") {
		if format == "int64" {
			return "BIGINT"
		}
		return "INTEGER"
	}
	if t.Is("number") {
		if format == "float" || format == "float32" {
			return "REAL"
		}
		return "DOUBLE PRECISION"
	}
	if t.Is("boolean") {
		return "BOOLEAN"
	}
	if t.Is("string") {
		return "TEXT"
	}
	if t.Is("array") {
		return "JSONB"
	}
	if t.Is("object") {
		return "JSONB"
	}
	return "TEXT"
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
