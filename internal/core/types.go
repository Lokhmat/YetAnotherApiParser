package core

import "context"

type FetchRequest struct {
	Method           string
	BaseURL          string
	Path             string
	PathParams       map[string]string
	QueryParams      map[string]string
	Headers          map[string]string
	SensitiveQuery   map[string]bool
	SensitiveHeaders map[string]bool
}

type FetchResult struct {
	Payload    []byte
	StatusCode int
	FinalURL   string
}

type APIConnector interface {
	Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)
}

type MigrationTarget interface {
	Apply(ctx context.Context, plan *MigrationPlan) (ApplyResult, error)
	ExportSQL(plan *MigrationPlan) ([]byte, error)
	Capabilities() Capabilities
}

type Capabilities struct {
	CanExportSQL bool
}

type ApplyResult struct {
	AppliedCount int
}

type OperationKind string

const (
	OperationCreateTable     OperationKind = "create_table"
	OperationCreateLinkTable OperationKind = "create_link_table"
	OperationInsertRows      OperationKind = "insert_rows"
)

type MigrationOperation interface {
	Kind() OperationKind
}

type MigrationPlan struct {
	Operations []MigrationOperation
}

func (p *MigrationPlan) Add(op MigrationOperation) {
	if op == nil {
		return
	}
	p.Operations = append(p.Operations, op)
}

type Column struct {
	Name       string
	Type       string
	Nullable   bool
	PrimaryKey bool
}

type CreateTableOp struct {
	TableName string
	Columns   []Column
}

func (CreateTableOp) Kind() OperationKind { return OperationCreateTable }

type CreateLinkTableOp struct {
	TableName   string
	LeftColumn  string
	LeftType    string
	RightColumn string
	RightType   string
	PrimaryKey  []string
}

func (CreateLinkTableOp) Kind() OperationKind { return OperationCreateLinkTable }

type Value struct {
	Scalar           interface{}
	Array            []interface{}
	ArrayElementType string
}

type InsertRow struct {
	Columns []string
	Values  []Value
}

type InsertRowsOp struct {
	TableName string
	Rows      []InsertRow
}

func (InsertRowsOp) Kind() OperationKind { return OperationInsertRows }
