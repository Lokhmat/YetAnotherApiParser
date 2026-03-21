package clickhouse

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
					{Name: "profile", Type: "JSONB", Nullable: true},
				},
			},
			core.CreateLinkTableOp{
				TableName:   "orders_tags_link",
				LeftColumn:  "orders_oid",
				LeftType:    "INTEGER",
				RightColumn: "tags_tid",
				RightType:   "INTEGER",
				PrimaryKey:  []string{"orders_oid", "tags_tid"},
			},
			core.InsertRowsOp{
				TableName: "users",
				Rows: []core.InsertRow{
					{
						Columns: []string{"id", "name", "phones", "profile"},
						Values: []core.Value{
							{Scalar: 1},
							{Scalar: "alice"},
							{Array: []interface{}{10, 11}, ArrayElementType: "INTEGER"},
							{Scalar: map[string]interface{}{"role": "admin"}},
						},
					},
					{
						Columns: []string{"id", "name"},
						Values: []core.Value{
							{Scalar: 2},
							{Scalar: "line1\nline2"},
						},
					},
					{
						Columns: []string{"id", "name"},
						Values: []core.Value{
							{Scalar: 1},
							{Scalar: "duplicate"},
						},
					},
				},
			},
		},
	}

	sqlBytes, err := (&adapter{}).ExportSQL(plan)
	if err != nil {
		t.Fatalf("ExportSQL returned error: %v", err)
	}
	sqlText := string(sqlBytes)
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS users",
		"id Int32",
		"name String",
		"phones Array(Int32)",
		"profile Nullable(String)",
		"ENGINE = MergeTree()",
		"ORDER BY (id);",
		"CREATE TABLE IF NOT EXISTS orders_tags_link",
		"ORDER BY (orders_oid, tags_tid);",
		"INSERT INTO users (id, name, phones, profile) VALUES (1, 'alice', [10, 11], '{\"role\":\"admin\"}');",
		"INSERT INTO users (id, name) VALUES (2, 'line1\\nline2');",
	} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("expected SQL to contain %q\nactual:\n%s", needle, sqlText)
		}
	}
	if strings.Contains(sqlText, "duplicate") {
		t.Fatalf("expected duplicate PK row to be omitted from exported SQL\nactual:\n%s", sqlText)
	}
}

func TestRenderCreateTableWithoutPrimaryKeyUsesTupleOrderBy(t *testing.T) {
	sqlText := renderCreateTable(core.CreateTableOp{
		TableName: "events",
		Columns: []core.Column{
			{Name: "payload", Type: "JSONB", Nullable: true},
		},
	})

	if !strings.Contains(sqlText, "ORDER BY tuple();") {
		t.Fatalf("expected ORDER BY tuple() for tables without PK\nactual:\n%s", sqlText)
	}
	if !strings.Contains(sqlText, "payload Nullable(String)") {
		t.Fatalf("expected JSONB column to map to Nullable(String)\nactual:\n%s", sqlText)
	}
}
