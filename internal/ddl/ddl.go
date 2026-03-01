package ddl

import (
	"fmt"
	"sort"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
)

type Table struct {
	Name       string
	Columns    []Column
	PrimaryKey string
}

type Column struct {
	Name       string
	Type       string
	Nullable   bool
	PrimaryKey bool
}

type RelationColumnSpec struct {
	Name string
	Type string
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

// GenerateDDLFromObjectSchema generates CREATE TABLE SQL for a marked object schema.
// It stores only scalar fields from schema and appends relation reference columns.
func GenerateDDLFromObjectSchema(schema *openapi3.Schema, tableName string, relationColumns []RelationColumnSpec) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("schema is nil")
	}
	if schema.Type == nil || !schema.Type.Is("object") {
		return "", fmt.Errorf("schema must be object")
	}

	table := Table{
		Name:    tableName,
		Columns: []Column{},
	}

	propNames := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		propNames = append(propNames, name)
	}
	sort.Strings(propNames)

	for _, propName := range propNames {
		propRef := schema.Properties[propName]
		if propRef == nil || propRef.Value == nil {
			continue
		}

		prop := propRef.Value
		if prop.Type != nil && (prop.Type.Is("object") || prop.Type.Is("array")) {
			// In marked mode, unmarked containers are ignored on entity table level.
			continue
		}

		colType := goTypeToSQLType(prop.Type, prop.Format)
		nullable := !contains(schema.Required, propName)

		if ext, ok := prop.Extensions["x-pk"].(bool); ok && ext {
			table.PrimaryKey = propName
		}

		table.Columns = append(table.Columns, Column{
			Name:       propName,
			Type:       colType,
			Nullable:   nullable,
			PrimaryKey: false,
		})
	}

	sort.Slice(relationColumns, func(i, j int) bool {
		return relationColumns[i].Name < relationColumns[j].Name
	})
	existingCols := make(map[string]bool, len(table.Columns))
	for _, col := range table.Columns {
		existingCols[col.Name] = true
	}
	for _, relCol := range relationColumns {
		if relCol.Name == "" || relCol.Type == "" || existingCols[relCol.Name] {
			continue
		}
		table.Columns = append(table.Columns, Column{
			Name:       relCol.Name,
			Type:       relCol.Type,
			Nullable:   true,
			PrimaryKey: false,
		})
	}

	for i := range table.Columns {
		table.Columns[i].PrimaryKey = table.Columns[i].Name == table.PrimaryKey
	}

	return generateCreateTableSQL(table), nil
}

func GenerateJoinTableDDL(tableName, leftColumn, leftType, rightColumn, rightType string) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (\n  %s %s NOT NULL,\n  %s %s NOT NULL,\n  PRIMARY KEY (%s, %s)\n);",
		tableName,
		leftColumn, leftType,
		rightColumn, rightType,
		leftColumn, rightColumn,
	)
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
