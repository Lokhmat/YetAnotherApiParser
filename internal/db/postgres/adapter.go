package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/db"
	_ "github.com/lib/pq"
)

type adapter struct {
	connectionString string
}

const checkpointTableName = "parser_checkpoints"

func init() {
	db.Register("postgres", func(cfg config.DatabaseConfig) (core.MigrationTarget, error) {
		return &adapter{connectionString: cfg.ConnectionString}, nil
	})
}

func (a *adapter) Capabilities() core.Capabilities {
	return core.Capabilities{CanExportSQL: true, CanFullSync: true}
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

func (a *adapter) LoadCheckpoint(ctx context.Context, key string) (*core.Checkpoint, error) {
	conn, err := a.open(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := ensureCheckpointTable(ctx, conn); err != nil {
		return nil, err
	}

	row := conn.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT checkpoint_key, operation_path, method, params_json, pagination_param, pagination_type, resume_value_json, updated_at FROM %s WHERE checkpoint_key = $1`,
		quoteIdentifier(checkpointTableName),
	), key)
	var checkpoint core.Checkpoint
	if err := row.Scan(
		&checkpoint.Key,
		&checkpoint.OperationPath,
		&checkpoint.Method,
		&checkpoint.ParamsJSON,
		&checkpoint.PaginationParam,
		&checkpoint.PaginationType,
		&checkpoint.ResumeValueJSON,
		&checkpoint.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}
	return &checkpoint, nil
}

func (a *adapter) SaveCheckpoints(ctx context.Context, checkpoints []core.Checkpoint) error {
	if len(checkpoints) == 0 {
		return nil
	}
	conn, err := a.open(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin checkpoint transaction: %w", err)
	}
	if err := ensureCheckpointTable(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	stmt := fmt.Sprintf(
		`INSERT INTO %s (checkpoint_key, operation_path, method, params_json, pagination_param, pagination_type, resume_value_json, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (checkpoint_key) DO UPDATE
SET operation_path = EXCLUDED.operation_path,
    method = EXCLUDED.method,
    params_json = EXCLUDED.params_json,
    pagination_param = EXCLUDED.pagination_param,
    pagination_type = EXCLUDED.pagination_type,
    resume_value_json = EXCLUDED.resume_value_json,
    updated_at = EXCLUDED.updated_at`,
		quoteIdentifier(checkpointTableName),
	)
	for _, checkpoint := range checkpoints {
		updatedAt := checkpoint.UpdatedAt
		if updatedAt.IsZero() {
			updatedAt = time.Now().UTC()
		}
		if _, err := tx.ExecContext(ctx, stmt,
			checkpoint.Key,
			checkpoint.OperationPath,
			checkpoint.Method,
			checkpoint.ParamsJSON,
			checkpoint.PaginationParam,
			checkpoint.PaginationType,
			checkpoint.ResumeValueJSON,
			updatedAt,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit checkpoints: %w", err)
	}
	return nil
}

func (a *adapter) ExportFullSyncSQL(plan *core.FullSyncPlan) ([]byte, error) {
	if plan == nil {
		return nil, nil
	}

	lines := make([]string, 0)
	for _, table := range plan.Tables {
		lines = append(lines, renderFullSyncCreateTable(table))
	}
	for _, table := range plan.Tables {
		lines = append(lines, renderFullSyncUpserts(table)...)
	}
	for i := len(plan.Tables) - 1; i >= 0; i-- {
		lines = append(lines, renderFullSyncDelete(plan.Tables[i]))
	}
	if len(lines) == 0 {
		return nil, nil
	}
	return []byte(strings.Join(lines, "\n\n")), nil
}

func (a *adapter) ApplyFullSync(ctx context.Context, plan *core.FullSyncPlan) (core.ApplyResult, error) {
	sqlBytes, err := a.ExportFullSyncSQL(plan)
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

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return core.ApplyResult{}, fmt.Errorf("begin transaction: %w", err)
	}
	applied := 0
	for _, stmt := range splitStatements(string(sqlBytes)) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			_ = tx.Rollback()
			return core.ApplyResult{AppliedCount: applied}, fmt.Errorf("exec full sync: %w", err)
		}
		applied++
	}
	if err := tx.Commit(); err != nil {
		return core.ApplyResult{AppliedCount: applied}, fmt.Errorf("commit full sync: %w", err)
	}
	return core.ApplyResult{AppliedCount: applied}, nil
}

func renderCreateTable(op core.CreateTableOp) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", quoteIdentifier(op.TableName)))
	for i, col := range op.Columns {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString(fmt.Sprintf("  %s %s", quoteIdentifier(col.Name), col.Type))
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
	leftPK, rightPK := op.LeftColumn, op.RightColumn
	if len(op.PrimaryKey) >= 2 {
		leftPK, rightPK = op.PrimaryKey[0], op.PrimaryKey[1]
	}
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s %s NOT NULL,\n  %s %s NOT NULL,\n  PRIMARY KEY (%s, %s)\n);",
		quoteIdentifier(op.TableName),
		quoteIdentifier(op.LeftColumn), op.LeftType,
		quoteIdentifier(op.RightColumn), op.RightType,
		quoteIdentifier(leftPK), quoteIdentifier(rightPK),
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
		conflictClause := buildPostgresConflictClause(row, pkColumns)
		lines = append(lines, fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)%s;",
			quoteIdentifier(op.TableName),
			quoteIdentifiers(row.Columns),
			strings.Join(values, ", "),
			conflictClause,
		))
	}
	return lines
}

func buildPostgresConflictClause(row core.InsertRow, pkColumns []string) string {
	if len(pkColumns) == 0 {
		return ""
	}
	updates := make([]string, 0, len(row.Columns))
	for _, column := range row.Columns {
		if containsString(pkColumns, column) {
			continue
		}
		updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdentifier(column), quoteIdentifier(column)))
	}
	if len(updates) == 0 {
		return fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", quoteIdentifiers(pkColumns))
	}
	return fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", quoteIdentifiers(pkColumns), strings.Join(updates, ", "))
}

func containsString(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func (a *adapter) open(ctx context.Context) (*sql.DB, error) {
	conn, err := sql.Open("postgres", a.connectionString)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return conn, nil
}

type checkpointExecer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

func ensureCheckpointTable(ctx context.Context, execer checkpointExecer) error {
	_, err := execer.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  checkpoint_key TEXT PRIMARY KEY,
  operation_path TEXT NOT NULL,
  method TEXT NOT NULL,
  params_json JSONB NOT NULL,
  pagination_param TEXT NOT NULL,
  pagination_type TEXT NOT NULL,
  resume_value_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);`, quoteIdentifier(checkpointTableName)))
	if err != nil {
		return fmt.Errorf("ensure checkpoint table: %w", err)
	}
	return nil
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

func renderFullSyncCreateTable(table core.FullSyncTable) string {
	return renderCreateTable(core.CreateTableOp{
		TableName: table.Name,
		Columns:   table.Columns,
	})
}

func renderFullSyncUpserts(table core.FullSyncTable) []string {
	lines := make([]string, 0, len(table.Rows))
	for _, row := range table.Rows {
		values := make([]string, 0, len(row.Values))
		for _, value := range row.Values {
			values = append(values, toSQLLiteral(value))
		}
		lines = append(lines, fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)%s;",
			quoteIdentifier(table.Name),
			quoteIdentifiers(row.Columns),
			strings.Join(values, ", "),
			renderConflictClause(table, row.Columns),
		))
	}
	return lines
}

func renderConflictClause(table core.FullSyncTable, rowColumns []string) string {
	if len(table.PrimaryKey) == 0 {
		return ""
	}
	updates := make([]string, 0)
	pkSet := make(map[string]struct{}, len(table.PrimaryKey))
	for _, pk := range table.PrimaryKey {
		pkSet[pk] = struct{}{}
	}
	for _, col := range rowColumns {
		if _, ok := pkSet[col]; ok {
			continue
		}
		quotedCol := quoteIdentifier(col)
		updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", quotedCol, quotedCol))
	}
	if len(updates) == 0 {
		return fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", quoteIdentifiers(table.PrimaryKey))
	}
	return fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", quoteIdentifiers(table.PrimaryKey), strings.Join(updates, ", "))
}

func renderFullSyncDelete(table core.FullSyncTable) string {
	if len(table.Rows) == 0 {
		return fmt.Sprintf("DELETE FROM %s;", quoteIdentifier(table.Name))
	}

	clauses := make([]string, 0, len(table.Rows))
	for _, row := range table.Rows {
		pkClauses := make([]string, 0, len(table.PrimaryKey))
		for _, pk := range table.PrimaryKey {
			index := indexOfColumn(row.Columns, pk)
			if index == -1 || index >= len(row.Values) {
				continue
			}
			pkClauses = append(pkClauses, fmt.Sprintf("%s = %s", quoteIdentifier(pk), toSQLLiteral(row.Values[index])))
		}
		if len(pkClauses) == 0 {
			continue
		}
		clauses = append(clauses, "("+strings.Join(pkClauses, " AND ")+")")
	}
	if len(clauses) == 0 {
		return fmt.Sprintf("DELETE FROM %s;", quoteIdentifier(table.Name))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE NOT (%s);", quoteIdentifier(table.Name), strings.Join(clauses, " OR "))
}

func indexOfColumn(columns []string, target string) int {
	for i, col := range columns {
		if col == target {
			return i
		}
	}
	return -1
}

func quoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, `"`+strings.ReplaceAll(part, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

func quoteIdentifiers(identifiers []string) string {
	quoted := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		quoted = append(quoted, quoteIdentifier(identifier))
	}
	return strings.Join(quoted, ", ")
}
