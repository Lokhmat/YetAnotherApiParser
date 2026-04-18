package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
)

type Service struct {
	api                 APIConnector
	compiledSpec        *openapi3.T
	operations          []*compiledOperation
	responseValues      map[string][]interface{}
	completedOneShotOps map[string]bool
}

func NewService(api APIConnector) *Service {
	return &Service{
		api:                 api,
		responseValues:      make(map[string][]interface{}),
		completedOneShotOps: make(map[string]bool),
	}
}

type containerKind string

const (
	containerObject containerKind = "object"
	containerArray  containerKind = "array"
)

type relationKind string

const (
	relationDirectRef relationKind = "direct_ref"
	relationLinkTable relationKind = "link_table"
)

const (
	filterOpIn  = "in"
	filterOpGT  = "gt"
	filterOpGTE = "gte"
	filterOpLT  = "lt"
	filterOpLTE = "lte"
)

type paramDataType string

const (
	paramDataTypeOperation paramDataType = "operation"
	paramDataTypeValues    paramDataType = "values"
	paramDataTypeCursor    paramDataType = "cursor"
	paramDataTypeOffset    paramDataType = "offset"
)

type incrementalStrategy string

const (
	incrementalStrategyHeadWatermark incrementalStrategy = "head-watermark"
)

type watermarkType string

const (
	watermarkTypeNumber   watermarkType = "number"
	watermarkTypeString   watermarkType = "string"
	watermarkTypeDateTime watermarkType = "datetime"
)

type ParamFilterSpec struct {
	Op     string
	Value  interface{}
	Values []interface{}
}

type OffsetParamSpec struct {
	Start     interface{}
	Increment interface{}
}

type ParamDataSpec struct {
	ParamName    string
	Type         paramDataType
	OperationID  string
	Values       []interface{}
	Filter       *ParamFilterSpec
	CursorPath   string
	OffsetConfig *OffsetParamSpec
}

type AuthParamSpec struct {
	ParamName string
	In        string
	EnvVar    string
}

type IncrementalSpec struct {
	Strategy           incrementalStrategy
	ItemsPath          string
	ItemsPathParts     []string
	ItemsPathIsRoot    bool
	WatermarkPath      string
	WatermarkPathParts []string
	WatermarkType      watermarkType
	KeyPaths           []string
	KeyPathParts       [][]string
}

type operationInfo struct {
	Path           string
	Op             *openapi3.Operation
	ParamSpecs     []ParamDataSpec
	PaginationSpec *ParamDataSpec
	AuthSpecs      []AuthParamSpec
	IsFetched      bool
	Plan           *operationExtractionPlan
}

type compiledOperation struct {
	Path            string
	Op              *openapi3.Operation
	ResourceType    ResourceType
	ParamSpecs      []ParamDataSpec
	PaginationSpec  *ParamDataSpec
	IncrementalSpec *IncrementalSpec
	AuthSpecs       []AuthParamSpec
	Plan            *operationExtractionPlan
	CreateOps       []MigrationOperation
	OwnedTables     []string
}

type headWatermarkCheckpointValue struct {
	Strategy       string      `json:"strategy"`
	WatermarkType  string      `json:"watermark_type"`
	WatermarkValue interface{} `json:"watermark_value"`
	BoundaryKeys   []string    `json:"boundary_keys"`
}

type headWatermarkCheckpointState struct {
	WatermarkType  watermarkType
	WatermarkValue interface{}
	BoundaryKeys   map[string]bool
}

type headWatermarkPageResult struct {
	FilteredPayload []byte
	RawItemCount    int
	StopAfterPage   bool
}

type headWatermarkCycleState struct {
	Observed      bool
	WatermarkType watermarkType
	MaxWatermark  interface{}
	BoundaryKeys  map[string]bool
}

type relationColumnPlan struct {
	Name     string
	Type     string
	IsArray  bool
	ElemType string
}

type markedNodePlan struct {
	Path            string
	TableName       string
	PKField         string
	PKSQLType       string
	Schema          *openapi3.Schema
	NodeKind        containerKind
	AccessPath      []string
	ParentPath      string
	ParentProp      string
	ParentNodeKind  containerKind
	ScalarColumns   []string
	RelationColumns map[string]relationColumnPlan
}

type relationPlan struct {
	Kind           relationKind
	ParentPath     string
	ChildPath      string
	ParentTable    string
	ChildTable     string
	ParentPKField  string
	ChildPKField   string
	ParentProp     string
	ParentNodeKind containerKind
	ChildNodeKind  containerKind
	RefSQLType     string
	IsArrayRef     bool
	JoinTableName  string
	JoinParentCol  string
	JoinParentType string
	JoinChildCol   string
	JoinChildType  string
}

type operationExtractionPlan struct {
	HasMarks       bool
	Nodes          []*markedNodePlan
	NodeByPath     map[string]*markedNodePlan
	ChildRelations map[string][]*relationPlan
	Relations      []*relationPlan
}

type tableState struct {
	table *FullSyncTable
	rows  map[string]InsertRow
}

var sqlIdentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (s *Service) GeneratePlan(ctx context.Context, spec *openapi3.T, baseURL string) (*MigrationPlan, error) {
	plan, err := s.GenerateCyclePlan(ctx, spec, baseURL, memoryCheckpointStore{})
	if err != nil {
		return nil, err
	}
	return mergeCyclePlan(plan)
}

func (s *Service) GenerateFullSyncPlan(ctx context.Context, spec *openapi3.T, baseURL string) (*FullSyncPlan, error) {
	plan, err := s.GeneratePlan(ctx, spec, baseURL)
	if err != nil {
		return nil, err
	}
	return buildFullSyncPlan(plan)
}

func (s *Service) GenerateCyclePlan(ctx context.Context, spec *openapi3.T, baseURL string, checkpoints MigrationTarget) (*CyclePlan, error) {
	if err := s.prepareSpec(spec); err != nil {
		return nil, err
	}

	upsertPlan := &MigrationPlan{}
	fullReloadPlan := &MigrationPlan{}
	workingResponseValues := cloneInterfaceMap(s.responseValues)
	fetchedThisCycle := make(map[string]bool, len(s.operations))
	createdForMode := map[ResourceType]map[string]bool{
		ResourceTypeOneShot:     make(map[string]bool),
		ResourceTypeIncremental: make(map[string]bool),
		ResourceTypeFullReload:  make(map[string]bool),
	}
	completedOneShotOps := make([]string, 0)
	pendingCheckpoints := make([]Checkpoint, 0)

	for {
		shouldContinue := false
		for _, opInfo := range s.operations {
			if fetchedThisCycle[opInfo.Path] {
				continue
			}
			if opInfo.ResourceType == ResourceTypeOneShot && s.completedOneShotOps[opInfo.Path] {
				fetchedThisCycle[opInfo.Path] = true
				continue
			}

			var combinations [][]interface{}
			if len(opInfo.ParamSpecs) == 0 {
				combinations = [][]interface{}{{}}
			} else {
				valueLists, ready, err := s.buildEffectiveParamValueLists(opInfo.Path, opInfo.ParamSpecs, workingResponseValues)
				if err != nil {
					return nil, err
				}
				if !ready {
					continue
				}
				combinations = s.generateCombinations(valueLists)
			}

			if len(combinations) == 0 {
				fetchedThisCycle[opInfo.Path] = true
				continue
			}

			targetPlan := upsertPlan
			if opInfo.ResourceType == ResourceTypeFullReload {
				targetPlan = fullReloadPlan
			}
			if !createdForMode[opInfo.ResourceType][opInfo.Path] {
				for _, createOp := range opInfo.CreateOps {
					targetPlan.Add(createOp)
				}
				createdForMode[opInfo.ResourceType][opInfo.Path] = true
			}

			for _, combo := range combinations {
				baseParams := s.buildParamsMapFromSpecs(opInfo.ParamSpecs, combo)
				requestParams, missingAuth := s.applyAuthParams(opInfo.Path, baseParams, opInfo.AuthSpecs)
				if missingAuth {
					continue
				}

				if opInfo.ResourceType == ResourceTypeIncremental && opInfo.IncrementalSpec != nil {
					checkpointKey, err := buildCheckpointKey("GET", opInfo.Path, baseParams, opInfo.PaginationSpec.ParamName)
					if err != nil {
						return nil, fmt.Errorf("build checkpoint key for %s: %w", opInfo.Path, err)
					}
					checkpoint, err := checkpoints.LoadCheckpoint(ctx, checkpointKey)
					if err != nil {
						return nil, fmt.Errorf("load checkpoint for %s: %w", opInfo.Path, err)
					}
					insertOps, pendingCheckpoint, err := s.fetchAndBuildHeadWatermarkInsertOps(
						ctx,
						baseURL,
						opInfo.Path,
						opInfo.Op,
						requestParams,
						baseParams,
						opInfo.PaginationSpec,
						opInfo.IncrementalSpec,
						workingResponseValues,
						opInfo.Plan,
						opInfo.AuthSpecs,
						checkpointKey,
						checkpoint,
					)
					if err != nil {
						log.Printf("failed to build INSERT ops for GET %s: %v", opInfo.Path, err)
						continue
					}
					for _, op := range insertOps {
						targetPlan.Add(op)
					}
					if pendingCheckpoint != nil {
						pendingCheckpoints = append(pendingCheckpoints, *pendingCheckpoint)
					}
					continue
				}

				startPaginationValue, hasStartPaginationValue := initialPaginationValue(opInfo.PaginationSpec)
				checkpointKey := ""
				if opInfo.ResourceType == ResourceTypeIncremental {
					if opInfo.PaginationSpec == nil {
						return nil, fmt.Errorf("incremental operation %s requires pagination", opInfo.Path)
					}
					generatedCheckpointKey, err := buildCheckpointKey("GET", opInfo.Path, baseParams, opInfo.PaginationSpec.ParamName)
					if err != nil {
						return nil, fmt.Errorf("build checkpoint key for %s: %w", opInfo.Path, err)
					}
					checkpointKey = generatedCheckpointKey
					checkpoint, err := checkpoints.LoadCheckpoint(ctx, checkpointKey)
					if err != nil {
						return nil, fmt.Errorf("load checkpoint for %s: %w", opInfo.Path, err)
					}
					if checkpoint != nil {
						resumeValue, compatible, err := parseResumeCheckpointValue(checkpoint.ResumeValueJSON)
						if err != nil {
							return nil, fmt.Errorf("parse checkpoint for %s: %w", opInfo.Path, err)
						}
						if compatible {
							startPaginationValue = resumeValue
							hasStartPaginationValue = true
						} else {
							log.Printf("warning: ignoring checkpoint for %s because it does not match resume-pagination incremental semantics", opInfo.Path)
						}
					}
				}

				insertOps, _, lastRequestedValue, hasLastRequestedValue, madeRequest, err := s.fetchAndBuildInsertOps(
					ctx,
					baseURL,
					opInfo.Path,
					opInfo.Op,
					requestParams,
					opInfo.PaginationSpec,
					workingResponseValues,
					opInfo.Plan,
					opInfo.AuthSpecs,
					startPaginationValue,
					hasStartPaginationValue,
				)
				if err != nil {
					log.Printf("failed to build INSERT ops for GET %s: %v", opInfo.Path, err)
					continue
				}
				for _, op := range insertOps {
					targetPlan.Add(op)
				}
				if opInfo.ResourceType == ResourceTypeIncremental && madeRequest && hasLastRequestedValue {
					checkpoint, err := buildCheckpoint("GET", opInfo.Path, baseParams, opInfo.PaginationSpec, checkpointKey, lastRequestedValue)
					if err != nil {
						return nil, fmt.Errorf("build checkpoint for %s: %w", opInfo.Path, err)
					}
					pendingCheckpoints = append(pendingCheckpoints, checkpoint)
				}
			}

			shouldContinue = true
			fetchedThisCycle[opInfo.Path] = true
		}

		if !shouldContinue {
			break
		}
	}

	if len(s.completedOneShotOps) < countOneShotOps(s.operations) {
		for _, opInfo := range s.operations {
			if opInfo.ResourceType != ResourceTypeOneShot || s.completedOneShotOps[opInfo.Path] {
				continue
			}
			completedOneShotOps = append(completedOneShotOps, opInfo.Path)
		}
	}

	relaxPlanNullability(upsertPlan)
	relaxPlanNullability(fullReloadPlan)
	fullSyncPlan, err := buildFullSyncPlan(fullReloadPlan)
	if err != nil {
		return nil, err
	}

	return &CyclePlan{
		UpsertPlan:          upsertPlan,
		FullSyncPlan:        fullSyncPlan,
		PendingCheckpoints:  pendingCheckpoints,
		nextResponseValues:  workingResponseValues,
		completedOneShotOps: completedOneShotOps,
	}, nil
}

func (s *Service) CommitCycle(plan *CyclePlan) {
	if plan == nil {
		return
	}
	if plan.nextResponseValues != nil {
		s.responseValues = cloneInterfaceMap(plan.nextResponseValues)
	}
	for _, path := range plan.completedOneShotOps {
		s.completedOneShotOps[path] = true
	}
}

func buildFullSyncPlan(plan *MigrationPlan) (*FullSyncPlan, error) {
	if plan == nil {
		return &FullSyncPlan{}, nil
	}

	states := make(map[string]*tableState)
	order := make([]string, 0)

	ensureTable := func(name string) *tableState {
		state := states[name]
		if state != nil {
			return state
		}
		table := &FullSyncTable{Name: name}
		state = &tableState{
			table: table,
			rows:  make(map[string]InsertRow),
		}
		states[name] = state
		order = append(order, name)
		return state
	}

	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case CreateTableOp:
			state := ensureTable(typed.TableName)
			state.table.Columns = append([]Column{}, typed.Columns...)
			state.table.PrimaryKey = primaryKeyColumnsFromColumns(typed.Columns)
		case *CreateTableOp:
			state := ensureTable(typed.TableName)
			state.table.Columns = append([]Column{}, typed.Columns...)
			state.table.PrimaryKey = primaryKeyColumnsFromColumns(typed.Columns)
		case CreateLinkTableOp:
			state := ensureTable(typed.TableName)
			state.table.Columns = []Column{
				{Name: typed.LeftColumn, Type: typed.LeftType, Nullable: false, PrimaryKey: true},
				{Name: typed.RightColumn, Type: typed.RightType, Nullable: false, PrimaryKey: true},
			}
			if len(typed.PrimaryKey) > 0 {
				state.table.PrimaryKey = append([]string{}, typed.PrimaryKey...)
			} else {
				state.table.PrimaryKey = []string{typed.LeftColumn, typed.RightColumn}
			}
		case *CreateLinkTableOp:
			state := ensureTable(typed.TableName)
			state.table.Columns = []Column{
				{Name: typed.LeftColumn, Type: typed.LeftType, Nullable: false, PrimaryKey: true},
				{Name: typed.RightColumn, Type: typed.RightType, Nullable: false, PrimaryKey: true},
			}
			if len(typed.PrimaryKey) > 0 {
				state.table.PrimaryKey = append([]string{}, typed.PrimaryKey...)
			} else {
				state.table.PrimaryKey = []string{typed.LeftColumn, typed.RightColumn}
			}
		case InsertRowsOp:
			if err := addFullSyncRows(ensureTable(typed.TableName), typed); err != nil {
				return nil, err
			}
		case *InsertRowsOp:
			if err := addFullSyncRows(ensureTable(typed.TableName), *typed); err != nil {
				return nil, err
			}
		}
	}

	result := &FullSyncPlan{Tables: make([]FullSyncTable, 0, len(order))}
	for _, name := range order {
		state := states[name]
		if len(state.table.PrimaryKey) == 0 {
			return nil, fmt.Errorf("full reload requires primary key metadata for table %s", name)
		}
		keys := make([]string, 0, len(state.rows))
		for key := range state.rows {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		rows := make([]InsertRow, 0, len(keys))
		for _, key := range keys {
			rows = append(rows, state.rows[key])
		}
		state.table.Rows = rows
		result.Tables = append(result.Tables, *state.table)
	}

	return result, nil
}

func addFullSyncRows(state *tableState, op InsertRowsOp) error {
	if state == nil {
		return nil
	}
	if len(state.table.PrimaryKey) == 0 {
		return fmt.Errorf("full reload requires primary key metadata for table %s", op.TableName)
	}
	for _, row := range op.Rows {
		key, err := buildFullSyncRowKey(op.TableName, row, state.table.PrimaryKey)
		if err != nil {
			return err
		}
		state.rows[key] = normalizeFullSyncRow(row, state.table.Columns)
	}
	return nil
}

func primaryKeyColumnsFromColumns(columns []Column) []string {
	result := make([]string, 0)
	for _, col := range columns {
		if col.PrimaryKey {
			result = append(result, col.Name)
		}
	}
	return result
}

func buildFullSyncRowKey(tableName string, row InsertRow, pkColumns []string) (string, error) {
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
			return "", fmt.Errorf("full reload row for table %s is missing primary key column %s", tableName, pkCol)
		}
		parts = append(parts, pkCol+"="+fullSyncValueKey(row.Values[index]))
	}
	return strings.Join(parts, "|"), nil
}

func fullSyncValueKey(v Value) string {
	if v.ArrayElementType != "" {
		return fmt.Sprintf("%v|%s", v.Array, v.ArrayElementType)
	}
	return fmt.Sprintf("%v", v.Scalar)
}

func normalizeFullSyncRow(row InsertRow, columns []Column) InsertRow {
	valueByColumn := make(map[string]Value, len(row.Columns))
	for i, col := range row.Columns {
		if i < len(row.Values) {
			valueByColumn[col] = row.Values[i]
		}
	}

	normalized := InsertRow{
		Columns: make([]string, 0, len(columns)),
		Values:  make([]Value, 0, len(columns)),
	}
	for _, col := range columns {
		normalized.Columns = append(normalized.Columns, col.Name)
		if value, ok := valueByColumn[col.Name]; ok {
			normalized.Values = append(normalized.Values, value)
			continue
		}
		normalized.Values = append(normalized.Values, Value{Scalar: nil})
	}
	return normalized
}

func relaxPlanNullability(plan *MigrationPlan) {
	if plan == nil {
		return
	}

	tableColumns := make(map[string][]Column)
	columnNullability := make(map[string]map[string]bool)
	markNullable := func(tableName, columnName string) {
		if _, ok := columnNullability[tableName]; !ok {
			columnNullability[tableName] = make(map[string]bool)
		}
		columnNullability[tableName][columnName] = true
	}

	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case CreateTableOp:
			tableColumns[typed.TableName] = append([]Column{}, typed.Columns...)
		case *CreateTableOp:
			tableColumns[typed.TableName] = append([]Column{}, typed.Columns...)
		}
	}

	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case InsertRowsOp:
			collectNullableColumns(typed.TableName, tableColumns[typed.TableName], typed.Rows, markNullable)
		case *InsertRowsOp:
			collectNullableColumns(typed.TableName, tableColumns[typed.TableName], typed.Rows, markNullable)
		}
	}

	if len(columnNullability) == 0 {
		return
	}

	for i, op := range plan.Operations {
		switch typed := op.(type) {
		case CreateTableOp:
			relaxCreateTableColumns(&typed, columnNullability)
			plan.Operations[i] = typed
		case *CreateTableOp:
			relaxCreateTableColumns(typed, columnNullability)
		}
	}
}

func collectNullableColumns(tableName string, columns []Column, rows []InsertRow, markNullable func(tableName, columnName string)) {
	for _, row := range rows {
		valueByColumn := make(map[string]Value, len(row.Columns))
		for i, col := range row.Columns {
			if i >= len(row.Values) {
				markNullable(tableName, col)
				continue
			}
			valueByColumn[col] = row.Values[i]
			if row.Values[i].ArrayElementType == "" && row.Values[i].Scalar == nil {
				markNullable(tableName, col)
			}
		}

		for _, column := range columns {
			if column.PrimaryKey {
				continue
			}
			if _, exists := valueByColumn[column.Name]; !exists {
				markNullable(tableName, column.Name)
			}
		}
	}
}

func relaxCreateTableColumns(op *CreateTableOp, nullableColumns map[string]map[string]bool) {
	if op == nil {
		return
	}
	tableNullable := nullableColumns[op.TableName]
	if len(tableNullable) == 0 {
		return
	}

	for i := range op.Columns {
		if op.Columns[i].PrimaryKey {
			continue
		}
		if tableNullable[op.Columns[i].Name] {
			op.Columns[i].Nullable = true
		}
	}
}

func buildCreateTableOpFromSchema(schemaRef *openapi3.SchemaRef, tableName string) (CreateTableOp, error) {
	if schemaRef == nil || schemaRef.Value == nil {
		return CreateTableOp{}, fmt.Errorf("schema is nil")
	}

	schema := schemaRef.Value
	if schema.Type != nil && schema.Type.Is("array") && schema.Items != nil {
		return buildCreateTableOpFromSchema(schema.Items, tableName)
	}
	if schema.Type == nil || !schema.Type.Is("object") || schema.Properties == nil {
		return CreateTableOp{}, fmt.Errorf("unsupported schema type: %v", schema.Type)
	}

	op := CreateTableOp{TableName: tableName}
	for _, propName := range sortedPropertyNames(schema) {
		propRef := schema.Properties[propName]
		if propRef == nil || propRef.Value == nil {
			continue
		}
		primaryKey := false
		if ext, ok := propRef.Value.Extensions["x-pk"].(bool); ok && ext {
			primaryKey = true
		}
		op.Columns = append(op.Columns, Column{
			Name:       propName,
			Type:       inferSQLType(propRef.Value.Type, propRef.Value.Format),
			Nullable:   !contains(schema.Required, propName),
			PrimaryKey: primaryKey,
		})
	}
	return op, nil
}

func buildCreateTableOpFromMarkedNode(node *markedNodePlan, relationColumns []Column) CreateTableOp {
	op := CreateTableOp{TableName: node.TableName}
	for _, propName := range sortedPropertyNames(node.Schema) {
		propRef := node.Schema.Properties[propName]
		if propRef == nil || propRef.Value == nil {
			continue
		}
		if propRef.Value.Type != nil && (propRef.Value.Type.Is("object") || propRef.Value.Type.Is("array")) {
			continue
		}
		op.Columns = append(op.Columns, Column{
			Name:       propName,
			Type:       inferSQLType(propRef.Value.Type, propRef.Value.Format),
			Nullable:   !contains(node.Schema.Required, propName),
			PrimaryKey: node.PKField == propName,
		})
	}
	op.Columns = append(op.Columns, relationColumns...)
	sort.Slice(op.Columns, func(i, j int) bool { return op.Columns[i].Name < op.Columns[j].Name })
	return op
}

func (s *Service) fetchAndBuildInsertOps(ctx context.Context, baseURL, path string, op *openapi3.Operation, params map[string]string, paginationSpec *ParamDataSpec, fetchedResponseValues map[string][]interface{}, plan *operationExtractionPlan, authSpecs []AuthParamSpec, startPaginationValue interface{}, hasStartPaginationValue bool) ([]MigrationOperation, bool, interface{}, bool, bool, error) {
	resp := s.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return nil, false, nil, false, false, fmt.Errorf("no successful response found")
	}

	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return nil, false, nil, false, false, fmt.Errorf("no JSON response schema found")
	}

	allInsertOps := make([]MigrationOperation, 0)
	paginationValue, hasPaginationValue := startPaginationValue, hasStartPaginationValue
	madeRequest := false
	lastRequestedValue := paginationValue
	hasLastRequestedValue := paginationSpec != nil

	for {
		requestParams := cloneStringMap(params)
		if paginationSpec != nil && hasPaginationValue {
			requestParams[paginationSpec.ParamName] = fmt.Sprintf("%v", paginationValue)
		}
		if paginationSpec != nil {
			lastRequestedValue = paginationValue
			hasLastRequestedValue = true
		}

		request, err := buildFetchRequest(baseURL, path, op, requestParams, authSpecs)
		if err != nil {
			return nil, false, nil, false, madeRequest, err
		}
		madeRequest = true
		result, err := s.api.Fetch(ctx, request)
		if err != nil {
			log.Printf("warning: failed to fetch data for %s: %v", path, err)
			return nil, false, nil, false, madeRequest, nil
		}

		s.extractResponseDataFromData(result.Payload, media, fetchedResponseValues)
		s.extractResponseDataFromMarkedData(result.Payload, media.Schema, fetchedResponseValues)

		pageInsertOps, hasRows, err := s.buildInsertOpsForPayload(result.Payload, resp, media, plan)
		if err != nil {
			return nil, false, nil, false, madeRequest, err
		}
		allInsertOps = append(allInsertOps, pageInsertOps...)

		if paginationSpec == nil {
			break
		}

		switch paginationSpec.Type {
		case paramDataTypeCursor:
			nextValue, ok := extractCursorValue(result.Payload, paginationSpec.CursorPath)
			if !ok {
				return allInsertOps, false, lastRequestedValue, hasLastRequestedValue, madeRequest, nil
			}
			if hasPaginationValue && fmt.Sprintf("%v", nextValue) == fmt.Sprintf("%v", paginationValue) {
				return allInsertOps, false, lastRequestedValue, hasLastRequestedValue, madeRequest, nil
			}
			paginationValue = nextValue
			hasPaginationValue = true
		case paramDataTypeOffset:
			if !hasRows {
				return allInsertOps, false, lastRequestedValue, hasLastRequestedValue, madeRequest, nil
			}
			nextValue, err := incrementOffsetValue(paginationValue, paginationSpec.OffsetConfig.Increment)
			if err != nil {
				return nil, false, nil, false, madeRequest, fmt.Errorf("%s parameter %s: %w", path, paginationSpec.ParamName, err)
			}
			paginationValue = nextValue
			hasPaginationValue = true
		default:
			return nil, false, nil, false, madeRequest, fmt.Errorf("unsupported pagination type %q", paginationSpec.Type)
		}
	}

	return allInsertOps, true, lastRequestedValue, hasLastRequestedValue, madeRequest, nil
}

func (s *Service) fetchAndBuildHeadWatermarkInsertOps(
	ctx context.Context,
	baseURL, path string,
	op *openapi3.Operation,
	requestParams map[string]string,
	checkpointParams map[string]string,
	paginationSpec *ParamDataSpec,
	incrementalSpec *IncrementalSpec,
	fetchedResponseValues map[string][]interface{},
	plan *operationExtractionPlan,
	authSpecs []AuthParamSpec,
	checkpointKey string,
	checkpoint *Checkpoint,
) ([]MigrationOperation, *Checkpoint, error) {
	resp := s.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return nil, nil, fmt.Errorf("no successful response found")
	}

	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return nil, nil, fmt.Errorf("no JSON response schema found")
	}

	var checkpointState *headWatermarkCheckpointState
	if checkpoint != nil {
		parsedState, compatible, err := parseHeadWatermarkCheckpoint(checkpoint.ResumeValueJSON)
		if err != nil {
			return nil, nil, err
		}
		if compatible {
			checkpointState = parsedState
		} else {
			log.Printf("warning: ignoring checkpoint for %s because it does not match head-watermark incremental semantics", path)
		}
	}

	allInsertOps := make([]MigrationOperation, 0)
	startPaginationValue, hasPaginationValue := initialPaginationValue(paginationSpec)
	paginationValue := startPaginationValue
	currentCycleState := &headWatermarkCycleState{
		WatermarkType: incrementalSpec.WatermarkType,
		BoundaryKeys:  map[string]bool{},
	}

	for {
		pageParams := cloneStringMap(requestParams)
		if hasPaginationValue {
			pageParams[paginationSpec.ParamName] = fmt.Sprintf("%v", paginationValue)
		}

		request, err := buildFetchRequest(baseURL, path, op, pageParams, authSpecs)
		if err != nil {
			return nil, nil, err
		}
		result, err := s.api.Fetch(ctx, request)
		if err != nil {
			log.Printf("warning: failed to fetch data for %s: %v", path, err)
			return nil, nil, nil
		}

		pageResult, err := filterHeadWatermarkPage(result.Payload, incrementalSpec, checkpointState, currentCycleState)
		if err != nil {
			return nil, nil, fmt.Errorf("filter head-watermark page: %w", err)
		}

		s.extractResponseDataFromData(pageResult.FilteredPayload, media, fetchedResponseValues)
		s.extractResponseDataFromMarkedData(pageResult.FilteredPayload, media.Schema, fetchedResponseValues)

		pageInsertOps, _, err := s.buildInsertOpsForPayload(pageResult.FilteredPayload, resp, media, plan)
		if err != nil {
			return nil, nil, err
		}
		allInsertOps = append(allInsertOps, pageInsertOps...)

		if pageResult.RawItemCount == 0 || pageResult.StopAfterPage {
			break
		}

		switch paginationSpec.Type {
		case paramDataTypeCursor:
			nextValue, ok := extractCursorValue(result.Payload, paginationSpec.CursorPath)
			if !ok {
				goto buildCheckpoint
			}
			if hasPaginationValue && fmt.Sprintf("%v", nextValue) == fmt.Sprintf("%v", paginationValue) {
				goto buildCheckpoint
			}
			paginationValue = nextValue
			hasPaginationValue = true
		case paramDataTypeOffset:
			nextValue, err := incrementOffsetValue(paginationValue, paginationSpec.OffsetConfig.Increment)
			if err != nil {
				return nil, nil, fmt.Errorf("%s parameter %s: %w", path, paginationSpec.ParamName, err)
			}
			paginationValue = nextValue
			hasPaginationValue = true
		default:
			return nil, nil, fmt.Errorf("unsupported pagination type %q", paginationSpec.Type)
		}
	}

buildCheckpoint:
	if !currentCycleState.Observed {
		return allInsertOps, nil, nil
	}
	if checkpointState != nil {
		if currentCycleState.WatermarkType != checkpointState.WatermarkType {
			log.Printf("warning: ignoring checkpoint advancement for %s because watermark types differ between stored checkpoint and spec", path)
			return allInsertOps, nil, nil
		}
		cmp, err := compareWatermarkValues(currentCycleState.WatermarkType, currentCycleState.MaxWatermark, checkpointState.WatermarkValue)
		if err != nil {
			return nil, nil, fmt.Errorf("compare head-watermark checkpoint: %w", err)
		}
		if cmp < 0 {
			log.Printf("warning: keeping existing checkpoint for %s because fetched head watermark moved backwards", path)
			return allInsertOps, nil, nil
		}
	}

	pendingCheckpoint, err := buildHeadWatermarkCheckpoint("GET", path, checkpointParams, paginationSpec, checkpointKey, currentCycleState)
	if err != nil {
		return nil, nil, err
	}
	return allInsertOps, &pendingCheckpoint, nil
}

func filterHeadWatermarkPage(payload []byte, spec *IncrementalSpec, checkpoint *headWatermarkCheckpointState, cycleState *headWatermarkCycleState) (*headWatermarkPageResult, error) {
	var decoded interface{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, fmt.Errorf("unmarshal JSON: %w", err)
	}

	itemsValue := decoded
	if !spec.ItemsPathIsRoot {
		itemsValue = resolveByPath(decoded, spec.ItemsPathParts)
	}
	rawItems, itemWasObject, err := listHeadWatermarkItems(itemsValue)
	if err != nil {
		return nil, err
	}
	filteredItems := make([]interface{}, 0, len(rawItems))
	allOlderThanCheckpoint := checkpoint != nil && len(rawItems) > 0

	for _, rawItem := range rawItems {
		item, ok := rawItem.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("x-incremental.items-path must resolve to objects")
		}

		watermarkValue := resolveByPath(item, spec.WatermarkPathParts)
		if watermarkValue == nil {
			return nil, fmt.Errorf("missing watermark value at %s", spec.WatermarkPath)
		}
		boundaryKey, err := canonicalizeBoundaryKey(item, spec.KeyPathParts)
		if err != nil {
			return nil, err
		}
		if err := updateHeadWatermarkCycleState(cycleState, spec.WatermarkType, watermarkValue, boundaryKey); err != nil {
			return nil, err
		}

		include := true
		if checkpoint != nil {
			if checkpoint.WatermarkType != spec.WatermarkType {
				return nil, fmt.Errorf("stored head-watermark checkpoint type %q does not match operation watermark type %q", checkpoint.WatermarkType, spec.WatermarkType)
			}
			cmp, err := compareWatermarkValues(spec.WatermarkType, watermarkValue, checkpoint.WatermarkValue)
			if err != nil {
				return nil, err
			}
			switch {
			case cmp > 0:
				allOlderThanCheckpoint = false
			case cmp == 0:
				allOlderThanCheckpoint = false
				include = !checkpoint.BoundaryKeys[boundaryKey]
			default:
				include = false
			}
		}
		if include {
			filteredItems = append(filteredItems, item)
		}
	}

	filteredRoot, err := setValueByPath(decoded, spec.ItemsPathParts, spec.ItemsPathIsRoot, filteredItems, itemWasObject)
	if err != nil {
		return nil, err
	}
	filteredPayload, err := json.Marshal(filteredRoot)
	if err != nil {
		return nil, fmt.Errorf("marshal filtered payload: %w", err)
	}

	return &headWatermarkPageResult{
		FilteredPayload: filteredPayload,
		RawItemCount:    len(rawItems),
		StopAfterPage:   allOlderThanCheckpoint,
	}, nil
}

func listHeadWatermarkItems(value interface{}) ([]interface{}, bool, error) {
	if value == nil {
		return nil, false, nil
	}
	if arrayItems, ok := value.([]interface{}); ok {
		return arrayItems, false, nil
	}
	if objectItem, ok := value.(map[string]interface{}); ok {
		return []interface{}{objectItem}, true, nil
	}
	return nil, false, fmt.Errorf("x-incremental.items-path must resolve to an object or array of objects")
}

func updateHeadWatermarkCycleState(state *headWatermarkCycleState, watermarkKind watermarkType, watermarkValue interface{}, boundaryKey string) error {
	if state == nil {
		return nil
	}
	if !state.Observed {
		state.Observed = true
		state.WatermarkType = watermarkKind
		state.MaxWatermark = watermarkValue
		if state.BoundaryKeys == nil {
			state.BoundaryKeys = map[string]bool{}
		}
		state.BoundaryKeys[boundaryKey] = true
		return nil
	}
	cmp, err := compareWatermarkValues(watermarkKind, watermarkValue, state.MaxWatermark)
	if err != nil {
		return err
	}
	switch {
	case cmp > 0:
		state.MaxWatermark = watermarkValue
		state.BoundaryKeys = map[string]bool{boundaryKey: true}
	case cmp == 0:
		state.BoundaryKeys[boundaryKey] = true
	}
	return nil
}

func buildFetchRequest(baseURL, path string, op *openapi3.Operation, params map[string]string, authSpecs []AuthParamSpec) (FetchRequest, error) {
	req := FetchRequest{
		Method:           "GET",
		BaseURL:          baseURL,
		Path:             path,
		PathParams:       map[string]string{},
		QueryParams:      map[string]string{},
		Headers:          map[string]string{},
		SensitiveQuery:   map[string]bool{},
		SensitiveHeaders: map[string]bool{},
	}

	for _, authSpec := range authSpecs {
		switch authSpec.In {
		case "query":
			req.SensitiveQuery[authSpec.ParamName] = true
		case "header":
			req.SensitiveHeaders[authSpec.ParamName] = true
		}
	}

	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		p := paramRef.Value
		val, ok := params[p.Name]
		if !ok {
			if p.Required {
				return FetchRequest{}, fmt.Errorf("missing required %s parameter: %s", p.In, p.Name)
			}
			continue
		}

		switch p.In {
		case "path":
			req.PathParams[p.Name] = val
		case "query":
			req.QueryParams[p.Name] = val
		case "header":
			req.Headers[p.Name] = val
		}
	}
	return req, nil
}

func (s *Service) getSchemaName(schemaRef *openapi3.SchemaRef) string {
	schemaName := "response"
	if schemaRef != nil && schemaRef.Ref != "" {
		parts := strings.Split(schemaRef.Ref, "/")
		if len(parts) > 0 {
			schemaName = parts[len(parts)-1]
		}
	}
	return schemaName
}

func (s *Service) prepareSpec(spec *openapi3.T) error {
	if spec == nil {
		return fmt.Errorf("spec is nil")
	}
	if s.compiledSpec == spec && s.operations != nil {
		return nil
	}

	operations, err := s.collectOperationsWithResType(spec)
	if err != nil {
		return err
	}
	tableModes := make(map[string]ResourceType)
	for _, opInfo := range operations {
		incrementalSpec, err := s.getIncrementalSpec(opInfo.Path, opInfo.Op, opInfo.ResourceType)
		if err != nil {
			return err
		}
		paramSpecs, paginationSpec, err := s.getParamDataSpecs(opInfo.Path, opInfo.Op)
		if err != nil {
			return fmt.Errorf("parse x-param-data for %s: %w", opInfo.Path, err)
		}
		if err := validateResponseDataHints(opInfo.Path, opInfo.Op); err != nil {
			return err
		}
		authSpecs, err := s.getAuthParamSpecs(opInfo.Path, opInfo.Op)
		if err != nil {
			return fmt.Errorf("parse x-auth for %s: %w", opInfo.Path, err)
		}
		extractionPlan, err := s.buildOperationExtractionPlan(opInfo.Op)
		if err != nil {
			return fmt.Errorf("build extraction plan for %s: %w", opInfo.Path, err)
		}
		createOps, ownedTables, err := s.buildOperationCreateOps(opInfo.Path, opInfo.Op, extractionPlan)
		if err != nil {
			return err
		}
		if opInfo.ResourceType == ResourceTypeIncremental && paginationSpec == nil {
			return fmt.Errorf("incremental operation %s requires cursor or offset pagination", opInfo.Path)
		}
		if incrementalSpec != nil && paginationSpec == nil {
			return fmt.Errorf("head-watermark incremental operation %s requires cursor or offset pagination", opInfo.Path)
		}
		if (opInfo.ResourceType == ResourceTypeIncremental || opInfo.ResourceType == ResourceTypeFullReload) && !ownedTablesHavePrimaryKeys(createOps, ownedTables) {
			return fmt.Errorf("%s requires primary key metadata for owned tables", opInfo.Path)
		}
		if incrementalSpec != nil {
			if err := validateIncrementalSpecAgainstOperation(opInfo.Path, opInfo.Op, incrementalSpec); err != nil {
				return err
			}
		}
		for _, tableName := range ownedTables {
			if existingMode, ok := tableModes[tableName]; ok && existingMode != opInfo.ResourceType {
				return fmt.Errorf("table %s is produced by mixed x-res-type modes: %s and %s", tableName, existingMode, opInfo.ResourceType)
			}
			tableModes[tableName] = opInfo.ResourceType
		}

		opInfo.ParamSpecs = paramSpecs
		opInfo.PaginationSpec = paginationSpec
		opInfo.IncrementalSpec = incrementalSpec
		opInfo.AuthSpecs = authSpecs
		opInfo.Plan = extractionPlan
		opInfo.CreateOps = createOps
		opInfo.OwnedTables = ownedTables
	}

	s.compiledSpec = spec
	s.operations = operations
	return nil
}

func (s *Service) buildOperationCreateOps(path string, op *openapi3.Operation, extractionPlan *operationExtractionPlan) ([]MigrationOperation, []string, error) {
	createOps := make([]MigrationOperation, 0)
	ownedTables := make([]string, 0)
	generatedTables := make(map[string]bool)

	if extractionPlan != nil && extractionPlan.HasMarks {
		for _, node := range extractionPlan.Nodes {
			if generatedTables[node.TableName] {
				continue
			}
			createOps = append(createOps, buildCreateTableOpFromMarkedNode(node, s.getRelationColumnsForNode(extractionPlan, node.Path)))
			ownedTables = append(ownedTables, node.TableName)
			generatedTables[node.TableName] = true
		}
		for _, rel := range extractionPlan.Relations {
			if rel.Kind != relationLinkTable || generatedTables[rel.JoinTableName] {
				continue
			}
			createOps = append(createOps, CreateLinkTableOp{
				TableName:   rel.JoinTableName,
				LeftColumn:  rel.JoinParentCol,
				LeftType:    rel.JoinParentType,
				RightColumn: rel.JoinChildCol,
				RightType:   rel.JoinChildType,
				PrimaryKey:  []string{rel.JoinParentCol, rel.JoinChildCol},
			})
			ownedTables = append(ownedTables, rel.JoinTableName)
			generatedTables[rel.JoinTableName] = true
		}
		return createOps, ownedTables, nil
	}

	resp := s.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return nil, nil, nil
	}
	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return nil, nil, nil
	}
	schemaName := s.getSchemaName(media.Schema)
	if schemaName == "" {
		return nil, nil, nil
	}
	createTable, err := buildCreateTableOpFromSchema(media.Schema, schemaName)
	if err != nil {
		return nil, nil, fmt.Errorf("build create table for %s: %w", path, err)
	}
	return []MigrationOperation{createTable}, []string{schemaName}, nil
}

func (s *Service) collectOperationsWithResType(spec *openapi3.T) ([]*compiledOperation, error) {
	var operations []*compiledOperation
	for path, pathItem := range spec.Paths.Map() {
		if pathItem == nil || pathItem.Get == nil {
			continue
		}
		if _, hasIncremental := pathItem.Get.Extensions["x-incremental"]; hasIncremental {
			rawType, ok := pathItem.Get.Extensions["x-res-type"]
			if !ok {
				return nil, fmt.Errorf("%s: x-incremental is allowed only when x-res-type=incremental", path)
			}
			resourceType, err := parseResourceType(rawType)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			if resourceType != ResourceTypeIncremental {
				return nil, fmt.Errorf("%s: x-incremental is allowed only when x-res-type=incremental", path)
			}
		}
		raw, ok := pathItem.Get.Extensions["x-res-type"]
		if !ok {
			continue
		}
		resourceType, err := parseResourceType(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		operations = append(operations, &compiledOperation{Path: path, Op: pathItem.Get, ResourceType: resourceType})
	}
	sort.Slice(operations, func(i, j int) bool { return operations[i].Path < operations[j].Path })
	return operations, nil
}

func (s *Service) getParamDataSpecs(opPath string, op *openapi3.Operation) ([]ParamDataSpec, *ParamDataSpec, error) {
	var specs []ParamDataSpec
	var paginationSpec *ParamDataSpec
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		if _, legacy := paramRef.Value.Extensions["x-fk"]; legacy {
			return nil, nil, fmt.Errorf("%s parameter %s: x-fk is no longer supported; use x-param-data", opPath, paramRef.Value.Name)
		}
		raw, ok := paramRef.Value.Extensions["x-param-data"]
		if !ok {
			continue
		}
		spec, err := parseParamDataSpec(raw, paramRef.Value.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("%s parameter %s: %w", opPath, paramRef.Value.Name, err)
		}
		if err := validateParamDataSpec(spec); err != nil {
			return nil, nil, fmt.Errorf("%s parameter %s: %w", opPath, paramRef.Value.Name, err)
		}
		if spec.Type == paramDataTypeCursor || spec.Type == paramDataTypeOffset {
			if paginationSpec != nil {
				return nil, nil, fmt.Errorf("%s defines multiple pagination parameters", opPath)
			}
			specCopy := spec
			paginationSpec = &specCopy
			continue
		}
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].ParamName < specs[j].ParamName })
	return specs, paginationSpec, nil
}

func (s *Service) getAuthParamSpecs(opPath string, op *openapi3.Operation) ([]AuthParamSpec, error) {
	var specs []AuthParamSpec
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		raw, ok := paramRef.Value.Extensions["x-auth"]
		if !ok {
			continue
		}
		spec, err := parseAuthParamSpec(raw, paramRef.Value.Name, paramRef.Value.In)
		if err != nil {
			return nil, fmt.Errorf("%s parameter %s: %w", opPath, paramRef.Value.Name, err)
		}
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool {
		if specs[i].In == specs[j].In {
			return specs[i].ParamName < specs[j].ParamName
		}
		return specs[i].In < specs[j].In
	})
	return specs, nil
}

func (s *Service) getIncrementalSpec(opPath string, op *openapi3.Operation, resourceType ResourceType) (*IncrementalSpec, error) {
	if op == nil {
		return nil, nil
	}
	raw, ok := op.Extensions["x-incremental"]
	if !ok {
		return nil, nil
	}
	if resourceType != ResourceTypeIncremental {
		return nil, fmt.Errorf("%s: x-incremental is allowed only when x-res-type=incremental", opPath)
	}
	spec, err := parseIncrementalSpec(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", opPath, err)
	}
	return spec, nil
}

func (s *Service) buildParamsMapFromSpecs(specs []ParamDataSpec, values []interface{}) map[string]string {
	params := make(map[string]string)
	for i, spec := range specs {
		params[spec.ParamName] = fmt.Sprintf("%v", values[i])
	}
	return params
}

func (s *Service) applyAuthParams(opPath string, params map[string]string, authSpecs []AuthParamSpec) (map[string]string, bool) {
	if len(authSpecs) == 0 {
		return params, false
	}
	resolved := make(map[string]string, len(params)+len(authSpecs))
	for k, v := range params {
		resolved[k] = v
	}
	for _, authSpec := range authSpecs {
		value, ok := os.LookupEnv(authSpec.EnvVar)
		if !ok {
			log.Printf("warning: skipping %s because auth env %s for parameter %s is not set", opPath, authSpec.EnvVar, authSpec.ParamName)
			return nil, true
		}
		resolved[authSpec.ParamName] = value
	}
	return resolved, false
}

func (s *Service) buildEffectiveParamValueLists(opPath string, specs []ParamDataSpec, fetchedResponseValues map[string][]interface{}) ([][]interface{}, bool, error) {
	valueLists := make([][]interface{}, 0, len(specs))
	for _, spec := range specs {
		values, ready, err := s.buildEffectiveParamValues(opPath, spec, fetchedResponseValues)
		if err != nil {
			return nil, false, err
		}
		if !ready || len(values) == 0 {
			return nil, false, nil
		}
		valueLists = append(valueLists, values)
	}
	return valueLists, true, nil
}

func (s *Service) generateCombinations(valueLists [][]interface{}) [][]interface{} {
	if len(valueLists) == 0 {
		return nil
	}
	result := make([][]interface{}, 0)
	for _, v := range valueLists[0] {
		result = append(result, []interface{}{v})
	}
	for i := 1; i < len(valueLists); i++ {
		next := make([][]interface{}, 0)
		for _, combo := range result {
			for _, value := range valueLists[i] {
				newCombo := make([]interface{}, len(combo)+1)
				copy(newCombo, combo)
				newCombo[len(combo)] = value
				next = append(next, newCombo)
			}
		}
		result = next
	}
	return result
}

func (s *Service) findResponseDataFields(media *openapi3.MediaType) map[string]string {
	fieldNames := make(map[string]string)
	schema := media.Schema.Value
	if schema == nil {
		return fieldNames
	}
	if schema.Type != nil && schema.Type.Is("array") && schema.Items != nil {
		schema = schema.Items.Value
	}
	if schema != nil && schema.Properties != nil {
		for propName, propRef := range schema.Properties {
			if propRef == nil || propRef.Value == nil {
				continue
			}
			if spec, ok, err := parseResponseDataSpec(propRef.Value.Extensions); err == nil && ok {
				fieldNames[spec] = propName
			}
		}
	}
	return fieldNames
}

func (s *Service) buildInsertOpsForPayload(data []byte, resp *openapi3.ResponseRef, media *openapi3.MediaType, plan *operationExtractionPlan) ([]MigrationOperation, bool, error) {
	if plan != nil && plan.HasMarks {
		insertOps, err := s.buildInsertOpsFromMarkedPlan(data, plan)
		if err != nil {
			log.Printf("warning: failed to build marked INSERT operations: %v", err)
			return nil, false, nil
		}
		return insertOps, len(insertOps) > 0, nil
	}

	schemaName := s.getSchemaName(media.Schema)
	expectedColumns := s.getExpectedColumns(resp)
	insertOp, err := s.buildInsertRowsOp(data, schemaName, expectedColumns)
	if err != nil {
		log.Printf("warning: failed to build INSERT operations: %v", err)
		return nil, false, nil
	}
	if len(insertOp.Rows) == 0 {
		return nil, false, nil
	}
	return []MigrationOperation{insertOp}, true, nil
}

func (s *Service) extractResponseDataFromData(data []byte, media *openapi3.MediaType, responseValues map[string][]interface{}) {
	if media == nil || media.Schema == nil {
		return
	}
	records, err := parseJSONRecords(data)
	if err != nil {
		return
	}
	fieldNames := s.findResponseDataFields(media)
	for _, record := range records {
		for responseID, responseField := range fieldNames {
			if val, exists := record[responseField]; exists {
				responseValues[responseID] = appendUniqueValue(responseValues[responseID], val)
			}
		}
	}
}

func (s *Service) extractResponseDataFromMarkedData(data []byte, schemaRef *openapi3.SchemaRef, responseValues map[string][]interface{}) {
	if schemaRef == nil || schemaRef.Value == nil {
		return
	}
	var payload interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return
	}

	var walk func(schema *openapi3.Schema, value interface{})
	walk = func(schema *openapi3.Schema, value interface{}) {
		if schema == nil || value == nil {
			return
		}
		if schema.Type != nil && schema.Type.Is("array") {
			arr, ok := value.([]interface{})
			if !ok {
				return
			}
			for _, item := range arr {
				if schema.Items != nil {
					walk(schema.Items.Value, item)
				}
			}
			return
		}
		if schema.Type != nil && schema.Type.Is("object") {
			obj, ok := value.(map[string]interface{})
			if !ok {
				return
			}
			for _, propName := range sortedPropertyNames(schema) {
				propRef := schema.Properties[propName]
				if propRef == nil || propRef.Value == nil {
					continue
				}
				propVal, exists := obj[propName]
				if !exists {
					continue
				}
				if responseID, ok, err := parseResponseDataSpec(propRef.Value.Extensions); err == nil && ok {
					responseValues[responseID] = appendUniqueValue(responseValues[responseID], propVal)
				}
				walk(propRef.Value, propVal)
			}
		}
	}
	walk(schemaRef.Value, payload)
}

func (s *Service) getExpectedColumns(resp *openapi3.ResponseRef) []string {
	if resp == nil || resp.Value == nil {
		return nil
	}
	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return nil
	}
	schema := media.Schema.Value
	if schema == nil {
		return nil
	}
	if schema.Type != nil && schema.Type.Is("array") && schema.Items != nil {
		schema = schema.Items.Value
	}
	var columns []string
	if schema != nil && schema.Properties != nil {
		for propName := range schema.Properties {
			columns = append(columns, propName)
		}
	}
	sort.Strings(columns)
	return columns
}

func (s *Service) buildInsertRowsOp(data []byte, tableName string, expectedColumns []string) (InsertRowsOp, error) {
	records, err := parseJSONRecords(data)
	if err != nil {
		return InsertRowsOp{}, fmt.Errorf("unmarshal JSON: %w", err)
	}
	op := InsertRowsOp{TableName: tableName}
	for _, record := range records {
		row := InsertRow{}
		for _, colName := range expectedColumns {
			if value, exists := record[colName]; exists {
				row.Columns = append(row.Columns, colName)
				row.Values = append(row.Values, Value{Scalar: value})
			}
		}
		if len(row.Columns) > 0 {
			op.Rows = append(op.Rows, row)
		}
	}
	return op, nil
}

func (s *Service) getSuccessResponse(op *openapi3.Operation) *openapi3.ResponseRef {
	if op == nil || op.Responses == nil {
		return nil
	}
	if resp := op.Responses.Value("200"); resp != nil {
		return resp
	}
	if resp := op.Responses.Value("201"); resp != nil {
		return resp
	}
	return nil
}

func parseJSONRecords(data []byte) ([]map[string]interface{}, error) {
	var arrayRecords []map[string]interface{}
	if err := json.Unmarshal(data, &arrayRecords); err == nil {
		return arrayRecords, nil
	}
	var singleRecord map[string]interface{}
	if err := json.Unmarshal(data, &singleRecord); err == nil {
		return []map[string]interface{}{singleRecord}, nil
	}
	return nil, fmt.Errorf("expected JSON object or array of objects")
}

func appendUniqueValue(values []interface{}, candidate interface{}) []interface{} {
	candidateKey := fmt.Sprintf("%v", candidate)
	for _, existing := range values {
		if fmt.Sprintf("%v", existing) == candidateKey {
			return values
		}
	}
	return append(values, candidate)
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return map[string]string{}
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneInterfaceMap(src map[string][]interface{}) map[string][]interface{} {
	if len(src) == 0 {
		return make(map[string][]interface{})
	}
	dst := make(map[string][]interface{}, len(src))
	for key, values := range src {
		dst[key] = append([]interface{}{}, values...)
	}
	return dst
}

func mergeCyclePlan(plan *CyclePlan) (*MigrationPlan, error) {
	merged := &MigrationPlan{}
	if plan == nil {
		return merged, nil
	}
	if plan.UpsertPlan != nil {
		for _, op := range plan.UpsertPlan.Operations {
			merged.Add(op)
		}
	}
	if plan.FullSyncPlan != nil {
		for _, table := range plan.FullSyncPlan.Tables {
			columns := append([]Column{}, table.Columns...)
			merged.Add(CreateTableOp{TableName: table.Name, Columns: columns})
			merged.Add(InsertRowsOp{TableName: table.Name, Rows: append([]InsertRow{}, table.Rows...)})
		}
	}
	return merged, nil
}

func countOneShotOps(operations []*compiledOperation) int {
	count := 0
	for _, opInfo := range operations {
		if opInfo.ResourceType == ResourceTypeOneShot {
			count++
		}
	}
	return count
}

func parseResourceType(raw interface{}) (ResourceType, error) {
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("x-res-type must be one of %q, %q, %q", ResourceTypeOneShot, ResourceTypeFullReload, ResourceTypeIncremental)
	}
	switch ResourceType(strings.ToLower(strings.TrimSpace(value))) {
	case ResourceTypeOneShot:
		return ResourceTypeOneShot, nil
	case ResourceTypeFullReload:
		return ResourceTypeFullReload, nil
	case ResourceTypeIncremental:
		return ResourceTypeIncremental, nil
	default:
		return "", fmt.Errorf("unsupported x-res-type %q", value)
	}
}

func ownedTablesHavePrimaryKeys(createOps []MigrationOperation, ownedTables []string) bool {
	if len(ownedTables) == 0 {
		return true
	}
	tablePKs := make(map[string]bool, len(ownedTables))
	for _, op := range createOps {
		switch typed := op.(type) {
		case CreateTableOp:
			tablePKs[typed.TableName] = len(primaryKeyColumnsFromColumns(typed.Columns)) > 0
		case *CreateTableOp:
			tablePKs[typed.TableName] = len(primaryKeyColumnsFromColumns(typed.Columns)) > 0
		case CreateLinkTableOp:
			tablePKs[typed.TableName] = len(linkTablePrimaryKeys(typed)) > 0
		case *CreateLinkTableOp:
			tablePKs[typed.TableName] = len(linkTablePrimaryKeys(*typed)) > 0
		}
	}
	for _, tableName := range ownedTables {
		if !tablePKs[tableName] {
			return false
		}
	}
	return true
}

func linkTablePrimaryKeys(op CreateLinkTableOp) []string {
	if len(op.PrimaryKey) > 0 {
		return append([]string{}, op.PrimaryKey...)
	}
	if op.LeftColumn == "" || op.RightColumn == "" {
		return nil
	}
	return []string{op.LeftColumn, op.RightColumn}
}

type memoryCheckpointStore map[string]Checkpoint

func (m memoryCheckpointStore) Apply(context.Context, *MigrationPlan) (ApplyResult, error) {
	return ApplyResult{}, nil
}

func (m memoryCheckpointStore) ApplyFullSync(context.Context, *FullSyncPlan) (ApplyResult, error) {
	return ApplyResult{}, nil
}

func (m memoryCheckpointStore) ExportSQL(*MigrationPlan) ([]byte, error) {
	return nil, nil
}

func (m memoryCheckpointStore) ExportFullSyncSQL(*FullSyncPlan) ([]byte, error) {
	return nil, nil
}

func (m memoryCheckpointStore) LoadCheckpoint(_ context.Context, key string) (*Checkpoint, error) {
	checkpoint, ok := m[key]
	if !ok {
		return nil, nil
	}
	copyCheckpoint := checkpoint
	return &copyCheckpoint, nil
}

func (m memoryCheckpointStore) SaveCheckpoints(_ context.Context, checkpoints []Checkpoint) error {
	for _, checkpoint := range checkpoints {
		m[checkpoint.Key] = checkpoint
	}
	return nil
}

func (m memoryCheckpointStore) Capabilities() Capabilities {
	return Capabilities{CanExportSQL: true, CanFullSync: true}
}

func buildCheckpointKey(method, path string, params map[string]string, paginationParam string) (string, error) {
	paramsJSON, err := serializeCheckpointParams(params)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s|%s|%s|%s", method, path, paginationParam, paramsJSON), nil
}

func buildCheckpoint(method, path string, params map[string]string, paginationSpec *ParamDataSpec, key string, resumeValue interface{}) (Checkpoint, error) {
	paramsJSON, err := serializeCheckpointParams(params)
	if err != nil {
		return Checkpoint{}, err
	}
	resumeValueJSON, err := json.Marshal(resumeValue)
	if err != nil {
		return Checkpoint{}, err
	}
	return Checkpoint{
		Key:             key,
		OperationPath:   path,
		Method:          method,
		ParamsJSON:      paramsJSON,
		PaginationParam: paginationSpec.ParamName,
		PaginationType:  string(paginationSpec.Type),
		ResumeValueJSON: string(resumeValueJSON),
	}, nil
}

func buildHeadWatermarkCheckpoint(method, path string, params map[string]string, paginationSpec *ParamDataSpec, key string, state *headWatermarkCycleState) (Checkpoint, error) {
	if state == nil || !state.Observed {
		return Checkpoint{}, fmt.Errorf("head-watermark checkpoint state is empty")
	}
	paramsJSON, err := serializeCheckpointParams(params)
	if err != nil {
		return Checkpoint{}, err
	}
	boundaryKeys := sortedBoundaryKeys(state.BoundaryKeys)
	resumeValueJSON, err := json.Marshal(headWatermarkCheckpointValue{
		Strategy:       string(incrementalStrategyHeadWatermark),
		WatermarkType:  string(state.WatermarkType),
		WatermarkValue: state.MaxWatermark,
		BoundaryKeys:   boundaryKeys,
	})
	if err != nil {
		return Checkpoint{}, err
	}
	return Checkpoint{
		Key:             key,
		OperationPath:   path,
		Method:          method,
		ParamsJSON:      paramsJSON,
		PaginationParam: paginationSpec.ParamName,
		PaginationType:  string(paginationSpec.Type),
		ResumeValueJSON: string(resumeValueJSON),
	}, nil
}

func serializeCheckpointParams(params map[string]string) (string, error) {
	if len(params) == 0 {
		return "{}", nil
	}
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make(map[string]string, len(keys))
	for _, key := range keys {
		ordered[key] = params[key]
	}
	data, err := json.Marshal(ordered)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func parseCheckpointResumeValue(raw string) (interface{}, error) {
	var value interface{}
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, err
	}
	return value, nil
}

func parseResumeCheckpointValue(raw string) (interface{}, bool, error) {
	value, err := parseCheckpointResumeValue(raw)
	if err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, true, nil
	}
	if _, ok := value.(map[string]interface{}); ok {
		return nil, false, nil
	}
	return value, true, nil
}

func parseHeadWatermarkCheckpoint(raw string) (*headWatermarkCheckpointState, bool, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, true, nil
	}
	var value interface{}
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, false, err
	}
	m, ok := value.(map[string]interface{})
	if !ok {
		return nil, false, nil
	}
	strategyValue, ok := m["strategy"].(string)
	if !ok || strings.TrimSpace(strategyValue) == "" {
		return nil, false, nil
	}
	if incrementalStrategy(strings.ToLower(strings.TrimSpace(strategyValue))) != incrementalStrategyHeadWatermark {
		return nil, false, nil
	}
	watermarkTypeValue, ok := m["watermark_type"].(string)
	if !ok || strings.TrimSpace(watermarkTypeValue) == "" {
		return nil, false, fmt.Errorf("head-watermark checkpoint watermark_type is required")
	}
	var normalizedType watermarkType
	switch watermarkType(strings.ToLower(strings.TrimSpace(watermarkTypeValue))) {
	case watermarkTypeNumber:
		normalizedType = watermarkTypeNumber
	case watermarkTypeString:
		normalizedType = watermarkTypeString
	case watermarkTypeDateTime:
		normalizedType = watermarkTypeDateTime
	default:
		return nil, false, fmt.Errorf("unsupported head-watermark checkpoint watermark_type %q", watermarkTypeValue)
	}
	state := &headWatermarkCheckpointState{
		WatermarkType:  normalizedType,
		WatermarkValue: m["watermark_value"],
		BoundaryKeys:   map[string]bool{},
	}
	boundaryRaw, ok := m["boundary_keys"]
	if !ok {
		return state, true, nil
	}
	boundaryList, ok := boundaryRaw.([]interface{})
	if !ok {
		return nil, false, fmt.Errorf("head-watermark checkpoint boundary_keys must be array")
	}
	for _, item := range boundaryList {
		boundaryKey, ok := item.(string)
		if !ok {
			return nil, false, fmt.Errorf("head-watermark checkpoint boundary_keys entries must be strings")
		}
		state.BoundaryKeys[boundaryKey] = true
	}
	return state, true, nil
}

func (s *Service) buildEffectiveParamValues(opPath string, spec ParamDataSpec, fetchedResponseValues map[string][]interface{}) ([]interface{}, bool, error) {
	switch spec.Type {
	case paramDataTypeOperation:
		values, ok := fetchedResponseValues[spec.OperationID]
		if !ok {
			return nil, false, nil
		}
		filtered, err := applyParamFilter(uniqueValues(values), spec.Filter)
		if err != nil {
			return nil, false, fmt.Errorf("%s parameter %s: %w", opPath, spec.ParamName, err)
		}
		return filtered, true, nil
	case paramDataTypeValues:
		return uniqueValues(spec.Values), true, nil
	default:
		return nil, false, fmt.Errorf("%s parameter %s: unsupported combination type %q", opPath, spec.ParamName, spec.Type)
	}
}

func parseAuthParamSpec(raw interface{}, paramName, in string) (AuthParamSpec, error) {
	envVar, ok := raw.(string)
	if !ok || strings.TrimSpace(envVar) == "" {
		return AuthParamSpec{}, fmt.Errorf("x-auth must be a non-empty string")
	}
	if in != "header" && in != "query" {
		return AuthParamSpec{}, fmt.Errorf("x-auth is supported only for header and query parameters")
	}
	return AuthParamSpec{
		ParamName: paramName,
		In:        in,
		EnvVar:    strings.TrimSpace(envVar),
	}, nil
}

func initialPaginationValue(spec *ParamDataSpec) (interface{}, bool) {
	if spec == nil {
		return nil, false
	}
	switch spec.Type {
	case paramDataTypeCursor:
		return nil, false
	case paramDataTypeOffset:
		if spec.OffsetConfig == nil {
			return nil, false
		}
		return spec.OffsetConfig.Start, true
	default:
		return nil, false
	}
}

func incrementOffsetValue(current, increment interface{}) (interface{}, error) {
	currentNum, ok := toNumber(current)
	if !ok {
		return nil, fmt.Errorf("offset value must be numeric")
	}
	incrementNum, ok := toNumber(increment)
	if !ok {
		return nil, fmt.Errorf("offset increment must be numeric")
	}
	return currentNum + incrementNum, nil
}

func extractCursorValue(data []byte, path string) (interface{}, bool) {
	var payload interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, false
	}
	current := payload
	for _, part := range strings.Split(path, ".") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, false
		}
		obj, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		next, ok := obj[part]
		if !ok {
			return nil, false
		}
		current = next
	}
	if current == nil {
		return nil, false
	}
	if s, ok := current.(string); ok && strings.TrimSpace(s) == "" {
		return nil, false
	}
	return current, true
}

func parseResponseDataSpec(extensions map[string]interface{}) (string, bool, error) {
	if _, legacy := extensions["x-fk"]; legacy {
		return "", false, fmt.Errorf("x-fk is no longer supported on response properties; use x-response-data")
	}
	raw, ok := extensions["x-response-data"]
	if !ok {
		return "", false, nil
	}
	specMap, ok := raw.(map[string]any)
	if !ok {
		return "", false, fmt.Errorf("x-response-data must be object")
	}
	idRaw, ok := specMap["id"]
	if !ok {
		return "", false, fmt.Errorf("x-response-data.id is required")
	}
	id, ok := idRaw.(string)
	if !ok || strings.TrimSpace(id) == "" {
		return "", false, fmt.Errorf("x-response-data.id must be non-empty string")
	}
	return strings.TrimSpace(id), true, nil
}

func validateResponseDataHints(opPath string, op *openapi3.Operation) error {
	resp := (&Service{}).getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return nil
	}
	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil || media.Schema.Value == nil {
		return nil
	}
	var walk func(schema *openapi3.Schema) error
	walk = func(schema *openapi3.Schema) error {
		if schema == nil {
			return nil
		}
		if _, _, err := parseResponseDataSpec(schema.Extensions); err != nil {
			return fmt.Errorf("%s response schema: %w", opPath, err)
		}
		if schema.Type != nil && schema.Type.Is("array") && schema.Items != nil {
			return walk(schema.Items.Value)
		}
		if schema.Type != nil && schema.Type.Is("object") {
			for _, propName := range sortedPropertyNames(schema) {
				propRef := schema.Properties[propName]
				if propRef == nil || propRef.Value == nil {
					continue
				}
				if _, _, err := parseResponseDataSpec(propRef.Value.Extensions); err != nil {
					return fmt.Errorf("%s response property %s: %w", opPath, propName, err)
				}
				if err := walk(propRef.Value); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(media.Schema.Value)
}

func parseIncrementalSpec(raw interface{}) (*IncrementalSpec, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("x-incremental must be object")
	}

	strategyRaw, ok := m["strategy"]
	if !ok {
		return nil, fmt.Errorf("x-incremental.strategy is required")
	}
	strategyValue, ok := strategyRaw.(string)
	if !ok || strings.TrimSpace(strategyValue) == "" {
		return nil, fmt.Errorf("x-incremental.strategy must be non-empty string")
	}

	spec := &IncrementalSpec{}
	switch incrementalStrategy(strings.ToLower(strings.TrimSpace(strategyValue))) {
	case incrementalStrategyHeadWatermark:
		spec.Strategy = incrementalStrategyHeadWatermark
	default:
		return nil, fmt.Errorf("unsupported x-incremental.strategy %q", strategyValue)
	}

	itemsPathRaw, ok := m["items-path"]
	if !ok {
		return nil, fmt.Errorf("x-incremental.items-path is required")
	}
	itemsPath, ok := itemsPathRaw.(string)
	if !ok || strings.TrimSpace(itemsPath) == "" {
		return nil, fmt.Errorf("x-incremental.items-path must be non-empty string")
	}
	itemsPathParts, isRoot, err := parseItemsPath(itemsPath)
	if err != nil {
		return nil, err
	}
	spec.ItemsPath = strings.TrimSpace(itemsPath)
	spec.ItemsPathParts = itemsPathParts
	spec.ItemsPathIsRoot = isRoot

	watermarkRaw, ok := m["watermark"]
	if !ok {
		return nil, fmt.Errorf("x-incremental.watermark is required")
	}
	watermarkMap, ok := watermarkRaw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("x-incremental.watermark must be object")
	}
	watermarkPathRaw, ok := watermarkMap["path"]
	if !ok {
		return nil, fmt.Errorf("x-incremental.watermark.path is required")
	}
	watermarkPath, ok := watermarkPathRaw.(string)
	if !ok || strings.TrimSpace(watermarkPath) == "" {
		return nil, fmt.Errorf("x-incremental.watermark.path must be non-empty string")
	}
	watermarkPathParts, err := parseDotPath(watermarkPath, "x-incremental.watermark.path")
	if err != nil {
		return nil, err
	}
	spec.WatermarkPath = strings.TrimSpace(watermarkPath)
	spec.WatermarkPathParts = watermarkPathParts

	watermarkTypeRaw, ok := watermarkMap["type"]
	if !ok {
		return nil, fmt.Errorf("x-incremental.watermark.type is required")
	}
	watermarkTypeValue, ok := watermarkTypeRaw.(string)
	if !ok || strings.TrimSpace(watermarkTypeValue) == "" {
		return nil, fmt.Errorf("x-incremental.watermark.type must be non-empty string")
	}
	switch watermarkType(strings.ToLower(strings.TrimSpace(watermarkTypeValue))) {
	case watermarkTypeNumber:
		spec.WatermarkType = watermarkTypeNumber
	case watermarkTypeString:
		spec.WatermarkType = watermarkTypeString
	case watermarkTypeDateTime:
		spec.WatermarkType = watermarkTypeDateTime
	default:
		return nil, fmt.Errorf("x-incremental.watermark.type must be one of %q, %q, %q", watermarkTypeNumber, watermarkTypeString, watermarkTypeDateTime)
	}

	keyPathsRaw, ok := m["key-paths"]
	if !ok {
		return nil, fmt.Errorf("x-incremental.key-paths is required")
	}
	keyPathList, ok := keyPathsRaw.([]interface{})
	if !ok || len(keyPathList) == 0 {
		return nil, fmt.Errorf("x-incremental.key-paths must be non-empty array")
	}
	seen := make(map[string]bool, len(keyPathList))
	for _, rawKeyPath := range keyPathList {
		keyPath, ok := rawKeyPath.(string)
		if !ok || strings.TrimSpace(keyPath) == "" {
			return nil, fmt.Errorf("x-incremental.key-paths entries must be non-empty strings")
		}
		normalized := strings.TrimSpace(keyPath)
		if seen[normalized] {
			return nil, fmt.Errorf("x-incremental.key-paths must not contain duplicates")
		}
		seen[normalized] = true
		keyPathParts, err := parseDotPath(normalized, "x-incremental.key-paths")
		if err != nil {
			return nil, err
		}
		spec.KeyPaths = append(spec.KeyPaths, normalized)
		spec.KeyPathParts = append(spec.KeyPathParts, keyPathParts)
	}

	return spec, nil
}

func validateIncrementalSpecAgainstOperation(opPath string, op *openapi3.Operation, spec *IncrementalSpec) error {
	if spec == nil {
		return nil
	}
	resp := (&Service{}).getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return fmt.Errorf("%s: x-incremental requires a successful JSON response schema", opPath)
	}
	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil || media.Schema.Value == nil {
		return fmt.Errorf("%s: x-incremental requires a successful JSON response schema", opPath)
	}

	itemsSchema := media.Schema
	var err error
	if !spec.ItemsPathIsRoot {
		itemsSchema, err = resolveSchemaPath(media.Schema, spec.ItemsPathParts)
		if err != nil {
			return fmt.Errorf("%s: invalid x-incremental.items-path: %w", opPath, err)
		}
	}
	itemSchema := itemsSchema.Value
	if itemSchema == nil {
		return fmt.Errorf("%s: invalid x-incremental.items-path", opPath)
	}
	if itemSchema.Type != nil && itemSchema.Type.Is("array") {
		if itemSchema.Items == nil || itemSchema.Items.Value == nil {
			return fmt.Errorf("%s: x-incremental.items-path must resolve to an array with item schema", opPath)
		}
		itemSchema = itemSchema.Items.Value
	}
	if itemSchema.Type == nil || !itemSchema.Type.Is("object") {
		return fmt.Errorf("%s: x-incremental.items-path must resolve to object items", opPath)
	}
	if _, err := resolveSchemaPath(&openapi3.SchemaRef{Value: itemSchema}, spec.WatermarkPathParts); err != nil {
		return fmt.Errorf("%s: invalid x-incremental.watermark.path: %w", opPath, err)
	}
	for _, keyPathParts := range spec.KeyPathParts {
		if _, err := resolveSchemaPath(&openapi3.SchemaRef{Value: itemSchema}, keyPathParts); err != nil {
			return fmt.Errorf("%s: invalid x-incremental.key-paths: %w", opPath, err)
		}
	}
	return nil
}

func parseParamDataSpec(raw interface{}, paramName string) (ParamDataSpec, error) {
	spec := ParamDataSpec{ParamName: paramName}
	m, ok := raw.(map[string]any)
	if !ok {
		return spec, fmt.Errorf("x-param-data must be object")
	}

	typeRaw, ok := m["type"]
	if !ok {
		return spec, fmt.Errorf("x-param-data.type is required")
	}
	typeValue, ok := typeRaw.(string)
	if !ok || strings.TrimSpace(typeValue) == "" {
		return spec, fmt.Errorf("x-param-data.type must be non-empty string")
	}
	spec.Type = paramDataType(strings.ToLower(strings.TrimSpace(typeValue)))

	switch spec.Type {
	case paramDataTypeOperation:
		opIDRaw, ok := m["operation-id"]
		if !ok {
			return spec, fmt.Errorf("x-param-data.operation-id is required for type=operation")
		}
		opID, ok := opIDRaw.(string)
		if !ok || strings.TrimSpace(opID) == "" {
			return spec, fmt.Errorf("x-param-data.operation-id must be non-empty string")
		}
		spec.OperationID = strings.TrimSpace(opID)
		if filterRaw, ok := m["filter"]; ok {
			filter, err := parseFilterSpec(filterRaw)
			if err != nil {
				return spec, err
			}
			spec.Filter = filter
		}
	case paramDataTypeValues:
		valuesRaw, ok := m["values"]
		if !ok {
			return spec, fmt.Errorf("x-param-data.values is required for type=values")
		}
		values, err := parseFilterValuesArray(valuesRaw, "x-param-data.values")
		if err != nil {
			return spec, err
		}
		spec.Values = uniqueValues(values)
	case paramDataTypeCursor:
		cursorRaw, ok := m["cursor"]
		if !ok {
			return spec, fmt.Errorf("x-param-data.cursor is required for type=cursor")
		}
		cursorPath, ok := cursorRaw.(string)
		if !ok || strings.TrimSpace(cursorPath) == "" {
			return spec, fmt.Errorf("x-param-data.cursor must be non-empty string")
		}
		spec.CursorPath = strings.TrimSpace(cursorPath)
	case paramDataTypeOffset:
		offsetRaw, ok := m["offset"]
		if !ok {
			return spec, fmt.Errorf("x-param-data.offset is required for type=offset")
		}
		offsetMap, ok := offsetRaw.(map[string]any)
		if !ok {
			return spec, fmt.Errorf("x-param-data.offset must be object")
		}
		start, ok := offsetMap["start"]
		if !ok {
			return spec, fmt.Errorf("x-param-data.offset.start is required")
		}
		increment, ok := offsetMap["increment"]
		if !ok {
			return spec, fmt.Errorf("x-param-data.offset.increment is required")
		}
		spec.OffsetConfig = &OffsetParamSpec{Start: start, Increment: increment}
	default:
		return spec, fmt.Errorf("unsupported x-param-data.type: %s", spec.Type)
	}
	return spec, nil
}

func validateParamDataSpec(spec ParamDataSpec) error {
	if strings.TrimSpace(spec.ParamName) == "" {
		return fmt.Errorf("parameter name is required")
	}
	switch spec.Type {
	case paramDataTypeOperation:
		if strings.TrimSpace(spec.OperationID) == "" {
			return fmt.Errorf("x-param-data.operation-id is required for type=operation")
		}
	case paramDataTypeValues:
		if len(spec.Values) == 0 {
			return fmt.Errorf("x-param-data.values must be non-empty")
		}
		for _, v := range spec.Values {
			if !isSupportedParamValue(v) {
				return fmt.Errorf("x-param-data.values supports only scalar or array values")
			}
		}
	case paramDataTypeCursor:
		if strings.TrimSpace(spec.CursorPath) == "" {
			return fmt.Errorf("x-param-data.cursor is required for type=cursor")
		}
	case paramDataTypeOffset:
		if spec.OffsetConfig == nil {
			return fmt.Errorf("x-param-data.offset is required for type=offset")
		}
		if _, ok := toNumber(spec.OffsetConfig.Start); !ok {
			return fmt.Errorf("x-param-data.offset.start must be numeric")
		}
		if _, ok := toNumber(spec.OffsetConfig.Increment); !ok {
			return fmt.Errorf("x-param-data.offset.increment must be numeric")
		}
	default:
		return fmt.Errorf("unsupported x-param-data.type: %s", spec.Type)
	}
	return nil
}

func parseFilterSpec(raw interface{}) (*ParamFilterSpec, error) {
	filterMap, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("x-param-data.filter must be object")
	}
	opRaw, ok := filterMap["op"]
	if !ok {
		return nil, fmt.Errorf("x-param-data.filter.op is required")
	}
	op, ok := opRaw.(string)
	if !ok || strings.TrimSpace(op) == "" {
		return nil, fmt.Errorf("x-param-data.filter.op must be non-empty string")
	}
	op = strings.ToLower(strings.TrimSpace(op))

	spec := &ParamFilterSpec{Op: op}
	switch op {
	case filterOpIn:
		valuesRaw, ok := filterMap["values"]
		if !ok {
			return nil, fmt.Errorf("x-param-data.filter.values is required for 'in'")
		}
		values, err := parseFilterValuesArray(valuesRaw, "x-param-data.filter.values")
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("x-param-data.filter.values must be non-empty")
		}
		if _, hasValue := filterMap["value"]; hasValue {
			return nil, fmt.Errorf("x-param-data.filter.value is not allowed for 'in'")
		}
		spec.Values = uniqueValues(values)
	case filterOpGT, filterOpGTE, filterOpLT, filterOpLTE:
		valueRaw, ok := filterMap["value"]
		if !ok {
			return nil, fmt.Errorf("x-param-data.filter.value is required for '%s'", op)
		}
		if !isScalarValue(valueRaw) {
			return nil, fmt.Errorf("x-param-data.filter.value must be scalar")
		}
		if _, hasValues := filterMap["values"]; hasValues {
			return nil, fmt.Errorf("x-param-data.filter.values is not allowed for '%s'", op)
		}
		if _, ok := toNumber(valueRaw); !ok {
			if _, ok := toRFC3339Time(valueRaw); !ok {
				return nil, fmt.Errorf("x-param-data.filter.value for '%s' must be numeric or RFC3339 datetime string", op)
			}
		}
		spec.Value = valueRaw
	default:
		return nil, fmt.Errorf("unsupported x-param-data.filter.op: %s", op)
	}
	return spec, nil
}

func applyParamFilter(values []interface{}, filter *ParamFilterSpec) ([]interface{}, error) {
	if filter == nil {
		return values, nil
	}
	filtered := make([]interface{}, 0, len(values))
	for _, candidate := range values {
		ok, err := matchParamFilter(candidate, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			filtered = append(filtered, candidate)
		}
	}
	return filtered, nil
}

func matchParamFilter(candidate interface{}, filter *ParamFilterSpec) (bool, error) {
	switch filter.Op {
	case filterOpIn:
		for _, accepted := range filter.Values {
			match, err := equalsFilterValue(candidate, accepted)
			if err != nil {
				return false, err
			}
			if match {
				return true, nil
			}
		}
		return false, nil
	case filterOpGT, filterOpGTE, filterOpLT, filterOpLTE:
		if thresholdNum, ok := toNumber(filter.Value); ok {
			candidateNum, ok := toNumber(candidate)
			if !ok {
				return false, fmt.Errorf("range filter %s expects numeric candidate, got %T", filter.Op, candidate)
			}
			return compareFloat(filter.Op, candidateNum, thresholdNum), nil
		}

		thresholdTime, ok := toRFC3339Time(filter.Value)
		if !ok {
			return false, fmt.Errorf("range filter %s has invalid threshold type", filter.Op)
		}
		candidateTime, ok := toRFC3339Time(candidate)
		if !ok {
			return false, fmt.Errorf("range filter %s expects RFC3339 datetime candidate, got %T", filter.Op, candidate)
		}
		return compareTime(filter.Op, candidateTime, thresholdTime), nil
	default:
		return false, fmt.Errorf("unsupported filter op: %s", filter.Op)
	}
}

func compareFloat(op string, candidate, threshold float64) bool {
	switch op {
	case filterOpGT:
		return candidate > threshold
	case filterOpGTE:
		return candidate >= threshold
	case filterOpLT:
		return candidate < threshold
	case filterOpLTE:
		return candidate <= threshold
	default:
		return false
	}
}

func compareTime(op string, candidate, threshold time.Time) bool {
	switch op {
	case filterOpGT:
		return candidate.After(threshold)
	case filterOpGTE:
		return candidate.After(threshold) || candidate.Equal(threshold)
	case filterOpLT:
		return candidate.Before(threshold)
	case filterOpLTE:
		return candidate.Before(threshold) || candidate.Equal(threshold)
	default:
		return false
	}
}

func equalsFilterValue(a, b interface{}) (bool, error) {
	aArr, aIsArr := a.([]interface{})
	bArr, bIsArr := b.([]interface{})
	if aIsArr || bIsArr {
		if !(aIsArr && bIsArr) {
			return false, fmt.Errorf("cannot compare array and non-array values in 'in' filter")
		}
		if len(aArr) != len(bArr) {
			return false, nil
		}
		for i := range aArr {
			eq, err := equalsFilterValue(aArr[i], bArr[i])
			if err != nil {
				return false, err
			}
			if !eq {
				return false, nil
			}
		}
		return true, nil
	}
	aNum, aIsNum := toNumber(a)
	bNum, bIsNum := toNumber(b)
	if aIsNum || bIsNum {
		if !(aIsNum && bIsNum) {
			return false, fmt.Errorf("cannot compare numeric and non-numeric values in 'in' filter")
		}
		return aNum == bNum, nil
	}
	aTime, aIsTime := toRFC3339Time(a)
	bTime, bIsTime := toRFC3339Time(b)
	if aIsTime || bIsTime {
		if !(aIsTime && bIsTime) {
			return false, fmt.Errorf("cannot compare RFC3339 and non-RFC3339 values in 'in' filter")
		}
		return aTime.Equal(bTime), nil
	}
	aBool, aIsBool := a.(bool)
	bBool, bIsBool := b.(bool)
	if aIsBool || bIsBool {
		if !(aIsBool && bIsBool) {
			return false, fmt.Errorf("cannot compare boolean and non-boolean values in 'in' filter")
		}
		return aBool == bBool, nil
	}
	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	if aIsStr || bIsStr {
		if !(aIsStr && bIsStr) {
			return false, fmt.Errorf("cannot compare string and non-string values in 'in' filter")
		}
		return aStr == bStr, nil
	}
	return false, fmt.Errorf("unsupported value type in 'in' filter: %T vs %T", a, b)
}

func parseFilterValuesArray(raw interface{}, fieldName string) ([]interface{}, error) {
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%s must be array", fieldName)
	}
	values := make([]interface{}, 0, len(items))
	for _, item := range items {
		if !isSupportedParamValue(item) {
			return nil, fmt.Errorf("%s supports only scalar or array values", fieldName)
		}
		values = append(values, item)
	}
	return values, nil
}

func isScalarValue(v interface{}) bool {
	switch v.(type) {
	case nil:
		return false
	case string, bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

func isSupportedParamValue(v interface{}) bool {
	if isScalarValue(v) {
		return true
	}
	arr, ok := v.([]interface{})
	if !ok {
		return false
	}
	for _, item := range arr {
		if !isSupportedParamValue(item) {
			return false
		}
	}
	return true
}

func toNumber(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}

func toRFC3339Time(v interface{}) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func inferSQLType(t *openapi3.Types, format string) string {
	if t == nil {
		return "TEXT"
	}
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
	if t.Is("array") || t.Is("object") {
		return "JSONB"
	}
	return "TEXT"
}

func isValidSQLIdentifier(name string) bool {
	return sqlIdentPattern.MatchString(name)
}

func normalizeSchemaSignature(schema *openapi3.Schema) (string, error) {
	b, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func getTableNameExtension(schema *openapi3.Schema) (string, bool) {
	if schema == nil {
		return "", false
	}
	v, ok := schema.Extensions["x-table-name"]
	if !ok {
		return "", false
	}
	tableName, ok := v.(string)
	if !ok || tableName == "" {
		return "", false
	}
	return tableName, true
}

func getSinglePK(schema *openapi3.Schema) (string, string, error) {
	if schema == nil {
		return "", "", fmt.Errorf("nil schema")
	}
	pkFields := make([]string, 0)
	pkTypes := make(map[string]string)
	for propName, propRef := range schema.Properties {
		if propRef == nil || propRef.Value == nil {
			continue
		}
		if ext, ok := propRef.Value.Extensions["x-pk"].(bool); ok && ext {
			pkFields = append(pkFields, propName)
			pkTypes[propName] = inferSQLType(propRef.Value.Type, propRef.Value.Format)
		}
	}
	sort.Strings(pkFields)
	if len(pkFields) != 1 {
		return "", "", fmt.Errorf("expected exactly one x-pk field, got %d", len(pkFields))
	}
	pk := pkFields[0]
	return pk, pkTypes[pk], nil
}

func copyPath(path []string) []string {
	res := make([]string, len(path))
	copy(res, path)
	return res
}

func isScalarSchema(schema *openapi3.Schema) bool {
	if schema == nil || schema.Type == nil {
		return true
	}
	return !schema.Type.Is("object") && !schema.Type.Is("array")
}

func sortedPropertyNames(schema *openapi3.Schema) []string {
	if schema == nil {
		return nil
	}
	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (s *Service) buildOperationExtractionPlan(op *openapi3.Operation) (*operationExtractionPlan, error) {
	resp := s.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return &operationExtractionPlan{HasMarks: false}, nil
	}
	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil || media.Schema.Value == nil {
		return &operationExtractionPlan{HasMarks: false}, nil
	}

	plan := &operationExtractionPlan{
		HasMarks:       false,
		Nodes:          []*markedNodePlan{},
		NodeByPath:     map[string]*markedNodePlan{},
		ChildRelations: map[string][]*relationPlan{},
		Relations:      []*relationPlan{},
	}
	tableSignatures := make(map[string]string)
	nodeCounter := 0

	var registerNode func(string, *openapi3.Schema, containerKind, []string, string, string, containerKind) (*markedNodePlan, error)
	registerNode = func(tableName string, nodeSchema *openapi3.Schema, nodeKind containerKind, accessPath []string, parentPath string, parentProp string, parentKind containerKind) (*markedNodePlan, error) {
		if !isValidSQLIdentifier(tableName) {
			return nil, fmt.Errorf("invalid x-table-name %q", tableName)
		}
		sig, err := normalizeSchemaSignature(nodeSchema)
		if err != nil {
			return nil, fmt.Errorf("schema signature for table %s: %w", tableName, err)
		}
		if prev, ok := tableSignatures[tableName]; ok && prev != sig {
			return nil, fmt.Errorf("x-table-name %q used for different schemas", tableName)
		}
		tableSignatures[tableName] = sig

		pkField, pkType, err := getSinglePK(nodeSchema)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", tableName, err)
		}

		nodePath := fmt.Sprintf("node:%03d", nodeCounter)
		nodeCounter++
		node := &markedNodePlan{
			Path:            nodePath,
			TableName:       tableName,
			PKField:         pkField,
			PKSQLType:       pkType,
			Schema:          nodeSchema,
			NodeKind:        nodeKind,
			AccessPath:      copyPath(accessPath),
			ParentPath:      parentPath,
			ParentProp:      parentProp,
			ParentNodeKind:  parentKind,
			ScalarColumns:   []string{},
			RelationColumns: map[string]relationColumnPlan{},
		}

		for _, propName := range sortedPropertyNames(nodeSchema) {
			propRef := nodeSchema.Properties[propName]
			if propRef == nil || propRef.Value == nil {
				continue
			}
			if isScalarSchema(propRef.Value) {
				node.ScalarColumns = append(node.ScalarColumns, propName)
			}
		}

		plan.Nodes = append(plan.Nodes, node)
		plan.NodeByPath[nodePath] = node
		plan.HasMarks = true

		if parentPath != "" {
			parentNode := plan.NodeByPath[parentPath]
			rel := &relationPlan{
				ParentPath:     parentPath,
				ChildPath:      nodePath,
				ParentTable:    parentNode.TableName,
				ChildTable:     node.TableName,
				ParentPKField:  parentNode.PKField,
				ChildPKField:   node.PKField,
				ParentProp:     parentProp,
				ParentNodeKind: parentNode.NodeKind,
				ChildNodeKind:  node.NodeKind,
			}
			if parentNode.NodeKind == containerArray && node.NodeKind == containerArray {
				rel.Kind = relationLinkTable
				rel.JoinTableName = fmt.Sprintf("%s_%s_link", parentNode.TableName, node.TableName)
				rel.JoinParentCol = fmt.Sprintf("%s_%s", parentNode.TableName, parentNode.PKField)
				rel.JoinParentType = parentNode.PKSQLType
				rel.JoinChildCol = fmt.Sprintf("%s_%s", node.TableName, node.PKField)
				rel.JoinChildType = node.PKSQLType
			} else {
				rel.Kind = relationDirectRef
				rel.IsArrayRef = node.NodeKind == containerArray
				rel.RefSQLType = node.PKSQLType
				if rel.IsArrayRef {
					rel.RefSQLType += "[]"
				}
				parentNode.RelationColumns[parentProp] = relationColumnPlan{
					Name:     parentProp,
					Type:     rel.RefSQLType,
					IsArray:  rel.IsArrayRef,
					ElemType: node.PKSQLType,
				}
			}
			plan.Relations = append(plan.Relations, rel)
			plan.ChildRelations[parentPath] = append(plan.ChildRelations[parentPath], rel)
		}

		return node, nil
	}

	var walk func(*openapi3.Schema, []string, string, containerKind, string) error
	walk = func(schema *openapi3.Schema, accessPath []string, ownerNodePath string, ownerNodeKind containerKind, ownerProp string) error {
		if schema == nil {
			return nil
		}
		if schema.Type != nil && schema.Type.Is("object") {
			if tableName, ok := getTableNameExtension(schema); ok {
				node, err := registerNode(tableName, schema, containerObject, accessPath, ownerNodePath, ownerProp, ownerNodeKind)
				if err != nil {
					return err
				}
				for _, propName := range sortedPropertyNames(schema) {
					propRef := schema.Properties[propName]
					if propRef == nil || propRef.Value == nil {
						continue
					}
					if err := walk(propRef.Value, append(copyPath(accessPath), propName), node.Path, node.NodeKind, propName); err != nil {
						return err
					}
				}
				return nil
			}
			for _, propName := range sortedPropertyNames(schema) {
				propRef := schema.Properties[propName]
				if propRef == nil || propRef.Value == nil {
					continue
				}
				nextOwnerProp := ownerProp
				if ownerNodePath == "" {
					nextOwnerProp = ""
				}
				if err := walk(propRef.Value, append(copyPath(accessPath), propName), ownerNodePath, ownerNodeKind, nextOwnerProp); err != nil {
					return err
				}
			}
			return nil
		}
		if schema.Type != nil && schema.Type.Is("array") {
			if schema.Items == nil || schema.Items.Value == nil {
				return nil
			}
			itemSchema := schema.Items.Value
			if itemSchema.Type != nil && itemSchema.Type.Is("object") {
				if tableName, ok := getTableNameExtension(itemSchema); ok {
					node, err := registerNode(tableName, itemSchema, containerArray, accessPath, ownerNodePath, ownerProp, ownerNodeKind)
					if err != nil {
						return err
					}
					for _, propName := range sortedPropertyNames(itemSchema) {
						propRef := itemSchema.Properties[propName]
						if propRef == nil || propRef.Value == nil {
							continue
						}
						if err := walk(propRef.Value, append(copyPath(accessPath), propName), node.Path, node.NodeKind, propName); err != nil {
							return err
						}
					}
					return nil
				}
			}
			return walk(itemSchema, accessPath, ownerNodePath, ownerNodeKind, ownerProp)
		}
		return nil
	}

	if err := walk(media.Schema.Value, []string{}, "", "", ""); err != nil {
		return nil, err
	}
	for _, rels := range plan.ChildRelations {
		sort.Slice(rels, func(i, j int) bool {
			if rels[i].ParentProp == rels[j].ParentProp {
				return rels[i].ChildPath < rels[j].ChildPath
			}
			return rels[i].ParentProp < rels[j].ParentProp
		})
	}
	sort.Slice(plan.Relations, func(i, j int) bool {
		if plan.Relations[i].ParentTable == plan.Relations[j].ParentTable {
			if plan.Relations[i].ChildTable == plan.Relations[j].ChildTable {
				return plan.Relations[i].ParentProp < plan.Relations[j].ParentProp
			}
			return plan.Relations[i].ChildTable < plan.Relations[j].ChildTable
		}
		return plan.Relations[i].ParentTable < plan.Relations[j].ParentTable
	})
	return plan, nil
}

func (s *Service) getRelationColumnsForNode(plan *operationExtractionPlan, nodePath string) []Column {
	node := plan.NodeByPath[nodePath]
	if node == nil || len(node.RelationColumns) == 0 {
		return nil
	}
	cols := make([]Column, 0, len(node.RelationColumns))
	for _, c := range node.RelationColumns {
		cols = append(cols, Column{Name: c.Name, Type: c.Type, Nullable: true})
	}
	sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })
	return cols
}

func resolveByPath(root interface{}, path []string) interface{} {
	cur := root
	for _, step := range path {
		obj, ok := cur.(map[string]interface{})
		if !ok {
			return nil
		}
		val, ok := obj[step]
		if !ok {
			return nil
		}
		cur = val
	}
	return cur
}

func setValueByPath(root interface{}, path []string, isRoot bool, filteredItems []interface{}, itemWasObject bool) (interface{}, error) {
	if isRoot {
		if itemWasObject {
			if len(filteredItems) == 0 {
				return nil, nil
			}
			return filteredItems[0], nil
		}
		return filteredItems, nil
	}
	if len(path) == 0 {
		return nil, fmt.Errorf("path is required")
	}
	parentPath := path[:len(path)-1]
	parentValue := resolveByPath(root, parentPath)
	parentObject, ok := parentValue.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("x-incremental.items-path parent must resolve to object")
	}
	if itemWasObject {
		if len(filteredItems) == 0 {
			parentObject[path[len(path)-1]] = nil
			return root, nil
		}
		parentObject[path[len(path)-1]] = filteredItems[0]
		return root, nil
	}
	parentObject[path[len(path)-1]] = filteredItems
	return root, nil
}

func parseItemsPath(raw string) ([]string, bool, error) {
	normalized := strings.TrimSpace(raw)
	if normalized == "$" {
		return nil, true, nil
	}
	parts, err := parseDotPath(normalized, "x-incremental.items-path")
	if err != nil {
		return nil, false, err
	}
	return parts, false, nil
}

func parseDotPath(raw, field string) ([]string, error) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("%s must be a dot-delimited path with non-empty segments", field)
		}
		result = append(result, part)
	}
	return result, nil
}

func resolveSchemaPath(schemaRef *openapi3.SchemaRef, path []string) (*openapi3.SchemaRef, error) {
	current := schemaRef
	if current == nil || current.Value == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	for _, step := range path {
		for current != nil && current.Value != nil && current.Value.Type != nil && current.Value.Type.Is("array") {
			current = current.Value.Items
		}
		if current == nil || current.Value == nil {
			return nil, fmt.Errorf("schema is nil")
		}
		if current.Value.Type == nil || !current.Value.Type.Is("object") {
			return nil, fmt.Errorf("segment %q is not reachable through object properties", step)
		}
		next, ok := current.Value.Properties[step]
		if !ok || next == nil || next.Value == nil {
			return nil, fmt.Errorf("segment %q not found", step)
		}
		current = next
	}
	return current, nil
}

func canonicalizeBoundaryKey(item map[string]interface{}, keyPathParts [][]string) (string, error) {
	values := make([]interface{}, 0, len(keyPathParts))
	for _, keyPath := range keyPathParts {
		value := resolveByPath(item, keyPath)
		if value == nil {
			return "", fmt.Errorf("missing boundary key value at %s", strings.Join(keyPath, "."))
		}
		values = append(values, value)
	}
	data, err := json.Marshal(values)
	if err != nil {
		return "", fmt.Errorf("marshal boundary key: %w", err)
	}
	return string(data), nil
}

func compareWatermarkValues(kind watermarkType, left, right interface{}) (int, error) {
	switch kind {
	case watermarkTypeNumber:
		leftValue, ok := toNumber(left)
		if !ok {
			return 0, fmt.Errorf("watermark value %v must be numeric", left)
		}
		rightValue, ok := toNumber(right)
		if !ok {
			return 0, fmt.Errorf("watermark value %v must be numeric", right)
		}
		switch {
		case leftValue < rightValue:
			return -1, nil
		case leftValue > rightValue:
			return 1, nil
		default:
			return 0, nil
		}
	case watermarkTypeString:
		leftValue, ok := left.(string)
		if !ok {
			return 0, fmt.Errorf("watermark value %v must be string", left)
		}
		rightValue, ok := right.(string)
		if !ok {
			return 0, fmt.Errorf("watermark value %v must be string", right)
		}
		switch {
		case leftValue < rightValue:
			return -1, nil
		case leftValue > rightValue:
			return 1, nil
		default:
			return 0, nil
		}
	case watermarkTypeDateTime:
		leftRaw, ok := left.(string)
		if !ok {
			return 0, fmt.Errorf("watermark value %v must be RFC3339 string", left)
		}
		rightRaw, ok := right.(string)
		if !ok {
			return 0, fmt.Errorf("watermark value %v must be RFC3339 string", right)
		}
		leftValue, err := time.Parse(time.RFC3339, leftRaw)
		if err != nil {
			return 0, fmt.Errorf("parse watermark %q: %w", leftRaw, err)
		}
		rightValue, err := time.Parse(time.RFC3339, rightRaw)
		if err != nil {
			return 0, fmt.Errorf("parse watermark %q: %w", rightRaw, err)
		}
		switch {
		case leftValue.Before(rightValue):
			return -1, nil
		case leftValue.After(rightValue):
			return 1, nil
		default:
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("unsupported watermark type %q", kind)
	}
}

func sortedBoundaryKeys(boundaryKeys map[string]bool) []string {
	keys := make([]string, 0, len(boundaryKeys))
	for key := range boundaryKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type markedInsertContext struct {
	plan         *operationExtractionPlan
	insertedRows map[string]map[string]bool
	insertedLink map[string]bool
	operations   []MigrationOperation
}

func (c *markedInsertContext) recordInsertedRow(tableName, pk string) bool {
	if _, ok := c.insertedRows[tableName]; !ok {
		c.insertedRows[tableName] = map[string]bool{}
	}
	if c.insertedRows[tableName][pk] {
		return false
	}
	c.insertedRows[tableName][pk] = true
	return true
}

func uniqueValues(values []interface{}) []interface{} {
	seen := map[string]bool{}
	result := make([]interface{}, 0, len(values))
	for _, value := range values {
		key := fmt.Sprintf("%v", value)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, value)
	}
	return result
}

func (s *Service) buildInsertOpsFromMarkedPlan(data []byte, plan *operationExtractionPlan) ([]MigrationOperation, error) {
	var payload interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal JSON: %w", err)
	}
	ctx := &markedInsertContext{
		plan:         plan,
		insertedRows: map[string]map[string]bool{},
		insertedLink: map[string]bool{},
		operations:   []MigrationOperation{},
	}
	for _, node := range plan.Nodes {
		if node.ParentPath != "" {
			continue
		}
		target := resolveByPath(payload, node.AccessPath)
		if target == nil && len(node.AccessPath) == 0 {
			target = payload
		}
		ctx.processNode(node, target)
	}
	return ctx.operations, nil
}

func (c *markedInsertContext) processNode(node *markedNodePlan, raw interface{}) []interface{} {
	if node == nil || raw == nil {
		return nil
	}
	switch node.NodeKind {
	case containerObject:
		record, ok := raw.(map[string]interface{})
		if !ok {
			log.Printf("warning: expected object for table %s", node.TableName)
			return nil
		}
		pk := c.processRecord(node, record)
		if pk == nil {
			return nil
		}
		return []interface{}{pk}
	case containerArray:
		arr, ok := raw.([]interface{})
		if !ok {
			if obj, single := raw.(map[string]interface{}); single {
				arr = []interface{}{obj}
			} else {
				log.Printf("warning: expected array for table %s", node.TableName)
				return nil
			}
		}
		pks := make([]interface{}, 0, len(arr))
		for _, item := range arr {
			record, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			pk := c.processRecord(node, record)
			if pk != nil {
				pks = append(pks, pk)
			}
		}
		return uniqueValues(pks)
	default:
		return nil
	}
}

func (c *markedInsertContext) processRecord(node *markedNodePlan, record map[string]interface{}) interface{} {
	rowValues := make(map[string]Value)
	for _, col := range node.ScalarColumns {
		if value, ok := record[col]; ok {
			rowValues[col] = Value{Scalar: value}
		}
	}

	linkRows := make([]*relationPlan, 0)
	linkChildValues := make(map[string][]interface{})
	for _, rel := range c.plan.ChildRelations[node.Path] {
		childNode := c.plan.NodeByPath[rel.ChildPath]
		if childNode == nil {
			continue
		}
		childRaw, exists := record[rel.ParentProp]
		if !exists {
			continue
		}
		childPKs := c.processNode(childNode, childRaw)
		if len(childPKs) == 0 {
			continue
		}
		if rel.Kind == relationDirectRef {
			if rel.IsArrayRef {
				rowValues[rel.ParentProp] = Value{Array: childPKs, ArrayElementType: childNode.PKSQLType}
			} else {
				rowValues[rel.ParentProp] = Value{Scalar: childPKs[0]}
			}
		} else if rel.Kind == relationLinkTable {
			linkRows = append(linkRows, rel)
			linkChildValues[rel.ChildPath] = childPKs
		}
	}

	pk, ok := record[node.PKField]
	if !ok {
		log.Printf("warning: missing x-pk field %s for table %s", node.PKField, node.TableName)
		return nil
	}
	pkKey := fmt.Sprintf("%v", pk)

	if c.recordInsertedRow(node.TableName, pkKey) {
		orderedColumns := make([]string, 0, len(node.ScalarColumns)+len(node.RelationColumns))
		orderedColumns = append(orderedColumns, node.ScalarColumns...)
		relNames := make([]string, 0, len(node.RelationColumns))
		for name := range node.RelationColumns {
			relNames = append(relNames, name)
		}
		sort.Strings(relNames)
		orderedColumns = append(orderedColumns, relNames...)

		row := InsertRow{}
		for _, col := range orderedColumns {
			value, exists := rowValues[col]
			if !exists {
				continue
			}
			row.Columns = append(row.Columns, col)
			row.Values = append(row.Values, value)
		}
		if len(row.Columns) > 0 {
			c.operations = append(c.operations, InsertRowsOp{
				TableName: node.TableName,
				Rows:      []InsertRow{row},
			})
		}
	}

	for _, rel := range linkRows {
		childPKs := uniqueValues(linkChildValues[rel.ChildPath])
		for _, childPK := range childPKs {
			linkKey := fmt.Sprintf("%s|%v|%v", rel.JoinTableName, pk, childPK)
			if c.insertedLink[linkKey] {
				continue
			}
			c.insertedLink[linkKey] = true
			c.operations = append(c.operations, InsertRowsOp{
				TableName: rel.JoinTableName,
				Rows: []InsertRow{{
					Columns: []string{rel.JoinParentCol, rel.JoinChildCol},
					Values: []Value{
						{Scalar: pk},
						{Scalar: childPK},
					},
				}},
			})
		}
	}
	return pk
}
