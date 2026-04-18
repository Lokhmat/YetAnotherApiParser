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
				Rows: []core.InsertRow{
					{
						Columns: []string{"id", "name", "phones"},
						Values: []core.Value{
							{Scalar: 1},
							{Scalar: "alice"},
							{Array: []interface{}{10, 11}, ArrayElementType: "INTEGER"},
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
		`CREATE TABLE IF NOT EXISTS "users"`,
		`"id" INTEGER PRIMARY KEY`,
		`"name" TEXT NOT NULL`,
		`"phones" INTEGER[]`,
		`CREATE TABLE IF NOT EXISTS "orders_tags_link"`,
		`PRIMARY KEY ("orders_oid", "tags_tid")`,
		`INSERT INTO "users" ("id", "name", "phones") VALUES (1, E'alice', ARRAY[10, 11]::INTEGER[]) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "phones" = EXCLUDED."phones";`,
		`INSERT INTO "users" ("id", "name") VALUES (2, E'line1\nline2') ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name";`,
	} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("expected SQL to contain %q\nactual:\n%s", needle, sqlText)
		}
	}
	if strings.Contains(sqlText, "duplicate") {
		t.Fatalf("expected duplicate PK row to be omitted from exported SQL\nactual:\n%s", sqlText)
	}
}

func TestExportFullSyncSQL(t *testing.T) {
	plan := &core.FullSyncPlan{
		Tables: []core.FullSyncTable{
			{
				Name: "users",
				Columns: []core.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "name", Type: "TEXT", Nullable: true},
				},
				PrimaryKey: []string{"id"},
				Rows: []core.InsertRow{
					{
						Columns: []string{"id", "name"},
						Values:  []core.Value{{Scalar: 1}, {Scalar: "alice"}},
					},
					{
						Columns: []string{"id", "name"},
						Values:  []core.Value{{Scalar: 2}, {Scalar: nil}},
					},
				},
			},
		},
	}

	sqlBytes, err := (&adapter{}).ExportFullSyncSQL(plan)
	if err != nil {
		t.Fatalf("ExportFullSyncSQL returned error: %v", err)
	}
	sqlText := string(sqlBytes)
	for _, needle := range []string{
		`CREATE TABLE IF NOT EXISTS "users"`,
		`INSERT INTO "users" ("id", "name") VALUES (1, E'alice') ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name";`,
		`INSERT INTO "users" ("id", "name") VALUES (2, NULL) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name";`,
		`DELETE FROM "users" WHERE NOT (("id" = 1) OR ("id" = 2));`,
	} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("expected SQL to contain %q\nactual:\n%s", needle, sqlText)
		}
	}
}

func TestExportFullSyncSQLDeletesAllWhenSnapshotEmpty(t *testing.T) {
	plan := &core.FullSyncPlan{
		Tables: []core.FullSyncTable{
			{
				Name:       "users",
				Columns:    []core.Column{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
				PrimaryKey: []string{"id"},
			},
		},
	}

	sqlBytes, err := (&adapter{}).ExportFullSyncSQL(plan)
	if err != nil {
		t.Fatalf("ExportFullSyncSQL returned error: %v", err)
	}
	if !strings.Contains(string(sqlBytes), `DELETE FROM "users";`) {
		t.Fatalf("expected delete-all statement, got:\n%s", sqlBytes)
	}
}

func TestExportFullSyncSQLQuotesReservedIdentifiers(t *testing.T) {
	plan := &core.FullSyncPlan{
		Tables: []core.FullSyncTable{
			{
				Name: "user",
				Columns: []core.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true},
					{Name: "select", Type: "TEXT", Nullable: true},
				},
				PrimaryKey: []string{"id"},
				Rows: []core.InsertRow{
					{
						Columns: []string{"id", "select"},
						Values:  []core.Value{{Scalar: 1}, {Scalar: "ok"}},
					},
				},
			},
		},
	}

	sqlBytes, err := (&adapter{}).ExportFullSyncSQL(plan)
	if err != nil {
		t.Fatalf("ExportFullSyncSQL returned error: %v", err)
	}
	sqlText := string(sqlBytes)
	for _, needle := range []string{
		`CREATE TABLE IF NOT EXISTS "user"`,
		`"select" TEXT`,
		`INSERT INTO "user" ("id", "select") VALUES (1, E'ok') ON CONFLICT ("id") DO UPDATE SET "select" = EXCLUDED."select";`,
		`DELETE FROM "user" WHERE NOT (("id" = 1));`,
	} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("expected SQL to contain %q\nactual:\n%s", needle, sqlText)
		}
	}
}
