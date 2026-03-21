package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/db"
	_ "github.com/lib/pq"
)

type adapter struct {
	connectionString string
}

func init() {
	db.Register("postgres", func(cfg config.DatabaseConfig) (core.MigrationTarget, error) {
		return &adapter{connectionString: cfg.ConnectionString}, nil
	})
}

func (a *adapter) Capabilities() core.Capabilities {
	return core.Capabilities{CanExportSQL: true}
}

func (a *adapter) ExportSQL(plan *core.MigrationPlan) ([]byte, error) {
	if plan == nil {
		return nil, nil
	}

	tablePKs := collectPrimaryKeys(plan)
	seenInsertKeys := make(map[string]map[string]struct{})
	lines := make([]string, 0, len(plan.Operations))
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case core.CreateTableOp:
			lines = append(lines, renderCreateTable(typed))
		case *core.CreateTableOp:
			lines = append(lines, renderCreateTable(*typed))
		case core.CreateLinkTableOp:
			lines = append(lines, renderCreateLinkTable(typed))
		case *core.CreateLinkTableOp:
			lines = append(lines, renderCreateLinkTable(*typed))
		case core.InsertRowsOp:
			lines = append(lines, renderInsertRows(typed, tablePKs[typed.TableName], seenInsertKeys)...)
		case *core.InsertRowsOp:
			lines = append(lines, renderInsertRows(*typed, tablePKs[typed.TableName], seenInsertKeys)...)
		default:
			return nil, fmt.Errorf("unsupported migration operation %T", op)
		}
	}

	if len(lines) == 0 {
		return nil, nil
	}
	return []byte(strings.Join(lines, "\n\n")), nil
}

func (a *adapter) Apply(ctx context.Context, plan *core.MigrationPlan) (core.ApplyResult, error) {
	sqlBytes, err := a.ExportSQL(plan)
	if err != nil {
		return core.ApplyResult{}, err
	}
	if len(sqlBytes) == 0 {
		return core.ApplyResult{}, nil
	}

	conn, err := sql.Open("postgres", a.connectionString)
	if err != nil {
		return core.ApplyResult{}, fmt.Errorf("open database: %w", err)
	}
	defer conn.Close()

	if err := conn.PingContext(ctx); err != nil {
		return core.ApplyResult{}, fmt.Errorf("ping database: %w", err)
	}

	applied := 0
	for _, stmt := range splitStatements(string(sqlBytes)) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return core.ApplyResult{AppliedCount: applied}, fmt.Errorf("exec migration: %w", err)
		}
		applied++
	}
	return core.ApplyResult{AppliedCount: applied}, nil
}

func renderCreateTable(op core.CreateTableOp) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", op.TableName))
	for i, col := range op.Columns {
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

func renderCreateLinkTable(op core.CreateLinkTableOp) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (\n  %s %s NOT NULL,\n  %s %s NOT NULL,\n  PRIMARY KEY (%s, %s)\n);",
		op.TableName,
		op.LeftColumn, op.LeftType,
		op.RightColumn, op.RightType,
		op.LeftColumn, op.RightColumn,
	)
}

func collectPrimaryKeys(plan *core.MigrationPlan) map[string][]string {
	result := make(map[string][]string)
	if plan == nil {
		return result
	}
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case core.CreateTableOp:
			for _, col := range typed.Columns {
				if col.PrimaryKey {
					result[typed.TableName] = append(result[typed.TableName], col.Name)
				}
			}
		case *core.CreateTableOp:
			for _, col := range typed.Columns {
				if col.PrimaryKey {
					result[typed.TableName] = append(result[typed.TableName], col.Name)
				}
			}
		case core.CreateLinkTableOp:
			result[typed.TableName] = append([]string{}, typed.PrimaryKey...)
		case *core.CreateLinkTableOp:
			result[typed.TableName] = append([]string{}, typed.PrimaryKey...)
		}
	}
	return result
}

func renderInsertRows(op core.InsertRowsOp, pkColumns []string, seenInsertKeys map[string]map[string]struct{}) []string {
	lines := make([]string, 0, len(op.Rows))
	conflictClause := ""
	if len(pkColumns) > 0 {
		conflictClause = fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(pkColumns, ", "))
	}
	for _, row := range op.Rows {
		if insertKey, ok := buildInsertDedupKey(op.TableName, row, pkColumns); ok {
			if _, exists := seenInsertKeys[op.TableName]; !exists {
				seenInsertKeys[op.TableName] = make(map[string]struct{})
			}
			if _, exists := seenInsertKeys[op.TableName][insertKey]; exists {
				continue
			}
			seenInsertKeys[op.TableName][insertKey] = struct{}{}
		}
		values := make([]string, 0, len(row.Values))
		for _, value := range row.Values {
			values = append(values, toSQLLiteral(value))
		}
		lines = append(lines, fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)%s;",
			op.TableName,
			strings.Join(row.Columns, ", "),
			strings.Join(values, ", "),
			conflictClause,
		))
	}
	return lines
}

func buildInsertDedupKey(tableName string, row core.InsertRow, pkColumns []string) (string, bool) {
	if len(pkColumns) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(pkColumns)+1)
	parts = append(parts, tableName)
	for _, pkCol := range pkColumns {
		index := -1
		for i, col := range row.Columns {
			if col == pkCol {
				index = i
				break
			}
		}
		if index == -1 || index >= len(row.Values) {
			return "", false
		}
		literal := toSQLLiteral(row.Values[index])
		parts = append(parts, pkCol+"="+literal)
	}
	return strings.Join(parts, "|"), true
}

func toSQLLiteral(v core.Value) string {
	if v.ArrayElementType != "" {
		return toSQLArrayLiteral(v.Array, v.ArrayElementType)
	}
	switch value := v.Scalar.(type) {
	case nil:
		return "NULL"
	case string:
		return quoteSQLString(value)
	case bool:
		if value {
			return "TRUE"
		}
		return "FALSE"
	case float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", value)
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return "NULL"
		}
		return quoteSQLString(string(encoded))
	}
}

func quoteSQLString(value string) string {
	escaped := strings.NewReplacer(
		"\\", "\\\\",
		"'", "''",
		"\n", "\\n",
		"\r", "\\r",
		"\t", "\\t",
	).Replace(value)
	return "E'" + escaped + "'"
}

func toSQLArrayLiteral(values []interface{}, elemType string) string {
	cleanElemType := strings.TrimSuffix(elemType, "[]")
	if len(values) == 0 {
		return fmt.Sprintf("ARRAY[]::%s[]", cleanElemType)
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, toSQLLiteral(core.Value{Scalar: value}))
	}
	return fmt.Sprintf("ARRAY[%s]::%s[]", strings.Join(parts, ", "), cleanElemType)
}

func splitStatements(sqlText string) []string {
	parts := strings.Split(sqlText, ";\n")
	stmts := make([]string, 0, len(parts))
	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt == "" {
			continue
		}
		if !strings.HasSuffix(stmt, ";") {
			stmt += ";"
		}
		stmts = append(stmts, stmt)
	}
	return stmts
}
