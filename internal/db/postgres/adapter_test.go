package postgres

import (
	"strings"
	"testing"

	"api-parser/internal/core"
)

func TestExportSQL(t *testing.T) {
	plan := &core.MigrationPlan{
		Operations: []core.MigrationOperation{
			core.CreateTableOp{
				TableName: "users",
				Columns: []core.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "name", Type: "TEXT", Nullable: false},
					{Name: "phones", Type: "INTEGER[]", Nullable: true},
				},
			},
			core.CreateLinkTableOp{
				TableName:   "orders_tags_link",
				LeftColumn:  "orders_oid",
				LeftType:    "INTEGER",
				RightColumn: "tags_tid",
				RightType:   "INTEGER",
			},
			core.InsertRowsOp{
				TableName: "users",
				Rows: []core.InsertRow{{
					Columns: []string{"id", "name", "phones"},
					Values: []core.Value{
						{Scalar: 1},
						{Scalar: "alice"},
						{Array: []interface{}{10, 11}, ArrayElementType: "INTEGER"},
					},
				}},
			},
		},
	}

	sqlBytes, err := (&adapter{}).ExportSQL(plan)
	if err != nil {
		t.Fatalf("ExportSQL returned error: %v", err)
	}
	sqlText := string(sqlBytes)
	for _, needle := range []string{
		"CREATE TABLE users",
		"id INTEGER PRIMARY KEY",
		"name TEXT NOT NULL",
		"phones INTEGER[]",
		"CREATE TABLE orders_tags_link",
		"PRIMARY KEY (orders_oid, tags_tid)",
		"INSERT INTO users (id, name, phones) VALUES (1, 'alice', ARRAY[10, 11]::INTEGER[]);",
	} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("expected SQL to contain %q\nactual:\n%s", needle, sqlText)
		}
	}
}
