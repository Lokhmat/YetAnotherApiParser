package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/db"
	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type adapter struct {
	connectionString string
}

func init() {
	db.Register("clickhouse", func(cfg config.DatabaseConfig) (core.MigrationTarget, error) {
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

	conn, err := sql.Open("clickhouse", a.connectionString)
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
	b.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", op.TableName))
	for i, col := range op.Columns {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString(fmt.Sprintf("  %s %s", col.Name, toClickHouseColumnType(col)))
	}
	b.WriteString("\n)\n")
	b.WriteString("ENGINE = MergeTree()\n")
	b.WriteString(fmt.Sprintf("ORDER BY %s;", renderOrderBy(primaryKeyColumns(op.Columns))))
	return b.String()
}

func renderCreateLinkTable(op core.CreateLinkTableOp) string {
	pkColumns := linkTablePrimaryKey(op)
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s %s,\n  %s %s\n)\nENGINE = MergeTree()\nORDER BY %s;",
		op.TableName,
		op.LeftColumn, translateType(op.LeftType),
		op.RightColumn, translateType(op.RightType),
		renderOrderBy(pkColumns),
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
			result[typed.TableName] = primaryKeyColumns(typed.Columns)
		case *core.CreateTableOp:
			result[typed.TableName] = primaryKeyColumns(typed.Columns)
		case core.CreateLinkTableOp:
			result[typed.TableName] = linkTablePrimaryKey(typed)
		case *core.CreateLinkTableOp:
			result[typed.TableName] = linkTablePrimaryKey(*typed)
		}
	}
	return result
}

func primaryKeyColumns(columns []core.Column) []string {
	result := make([]string, 0, len(columns))
	for _, col := range columns {
		if col.PrimaryKey {
			result = append(result, col.Name)
		}
	}
	return result
}

func linkTablePrimaryKey(op core.CreateLinkTableOp) []string {
	if len(op.PrimaryKey) > 0 {
		return append([]string{}, op.PrimaryKey...)
	}
	return []string{op.LeftColumn, op.RightColumn}
}

func renderInsertRows(op core.InsertRowsOp, pkColumns []string, seenInsertKeys map[string]map[string]struct{}) []string {
	lines := make([]string, 0, len(op.Rows))
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
			"INSERT INTO %s (%s) VALUES (%s);",
			op.TableName,
			strings.Join(row.Columns, ", "),
			strings.Join(values, ", "),
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

func toClickHouseColumnType(col core.Column) string {
	base := translateType(col.Type)
	if col.PrimaryKey {
		return base
	}
	if col.Nullable && allowsNullable(base) {
		return "Nullable(" + base + ")"
	}
	return base
}

func allowsNullable(typ string) bool {
	return !strings.HasPrefix(typ, "Array(")
}

func translateType(sqlType string) string {
	cleanType := strings.TrimSpace(strings.ToUpper(sqlType))
	if strings.HasSuffix(cleanType, "[]") {
		return "Array(" + translateType(strings.TrimSuffix(cleanType, "[]")) + ")"
	}
	switch cleanType {
	case "INTEGER":
		return "Int32"
	case "BIGINT":
		return "Int64"
	case "REAL":
		return "Float32"
	case "DOUBLE PRECISION":
		return "Float64"
	case "BOOLEAN":
		return "Bool"
	case "TEXT", "JSONB":
		return "String"
	default:
		return "String"
	}
}

func renderOrderBy(pkColumns []string) string {
	if len(pkColumns) == 0 {
		return "tuple()"
	}
	return "(" + strings.Join(pkColumns, ", ") + ")"
}

func toSQLLiteral(v core.Value) string {
	if v.ArrayElementType != "" {
		return toSQLArrayLiteral(v.Array)
	}
	switch value := v.Scalar.(type) {
	case nil:
		return "NULL"
	case string:
		return quoteSQLString(value)
	case bool:
		if value {
			return "1"
		}
		return "0"
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
		"'", "\\'",
		"\n", "\\n",
		"\r", "\\r",
		"\t", "\\t",
	).Replace(value)
	return "'" + escaped + "'"
}

func toSQLArrayLiteral(values []interface{}) string {
	if len(values) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, toSQLLiteral(core.Value{Scalar: value}))
	}
	return "[" + strings.Join(parts, ", ") + "]"
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
