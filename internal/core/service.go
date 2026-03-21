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
	api APIConnector
}

func NewService(api APIConnector) *Service {
	return &Service{api: api}
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

type operationInfo struct {
	Path           string
	Op             *openapi3.Operation
	ParamSpecs     []ParamDataSpec
	PaginationSpec *ParamDataSpec
	AuthSpecs      []AuthParamSpec
	IsFetched      bool
	Plan           *operationExtractionPlan
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

var sqlIdentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (s *Service) GeneratePlan(ctx context.Context, spec *openapi3.T, baseURL string) (*MigrationPlan, error) {
	plan := &MigrationPlan{}
	operationDependencies := make(map[string]*operationInfo)
	generatedTables := make(map[string]bool)

	operations := s.collectOperationsWithResType(spec)
	for _, opInfo := range operations {
		paramSpecs, paginationSpec, err := s.getParamDataSpecs(opInfo.path, opInfo.op)
		if err != nil {
			return nil, fmt.Errorf("parse x-param-data for %s: %w", opInfo.path, err)
		}
		if err := validateResponseDataHints(opInfo.path, opInfo.op); err != nil {
			return nil, err
		}
		authSpecs, err := s.getAuthParamSpecs(opInfo.path, opInfo.op)
		if err != nil {
			return nil, fmt.Errorf("parse x-auth for %s: %w", opInfo.path, err)
		}
		extractionPlan, err := s.buildOperationExtractionPlan(opInfo.op)
		if err != nil {
			return nil, fmt.Errorf("build extraction plan for %s: %w", opInfo.path, err)
		}

		operationDependencies[opInfo.path] = &operationInfo{
			Path:           opInfo.path,
			Op:             opInfo.op,
			ParamSpecs:     paramSpecs,
			PaginationSpec: paginationSpec,
			AuthSpecs:      authSpecs,
			Plan:           extractionPlan,
			IsFetched:      false,
		}

		if extractionPlan != nil && extractionPlan.HasMarks {
			for _, node := range extractionPlan.Nodes {
				if generatedTables[node.TableName] {
					continue
				}
				plan.Add(buildCreateTableOpFromMarkedNode(node, s.getRelationColumnsForNode(extractionPlan, node.Path)))
				generatedTables[node.TableName] = true
			}
			for _, rel := range extractionPlan.Relations {
				if rel.Kind != relationLinkTable || generatedTables[rel.JoinTableName] {
					continue
				}
				plan.Add(CreateLinkTableOp{
					TableName:   rel.JoinTableName,
					LeftColumn:  rel.JoinParentCol,
					LeftType:    rel.JoinParentType,
					RightColumn: rel.JoinChildCol,
					RightType:   rel.JoinChildType,
					PrimaryKey:  []string{rel.JoinParentCol, rel.JoinChildCol},
				})
				generatedTables[rel.JoinTableName] = true
			}
			continue
		}

		resp := s.getSuccessResponse(opInfo.op)
		if resp == nil || resp.Value == nil {
			continue
		}
		media := resp.Value.Content.Get("application/json")
		if media == nil || media.Schema == nil {
			continue
		}
		schemaName := s.getSchemaName(media.Schema)
		if schemaName == "" || generatedTables[schemaName] {
			continue
		}
		createTable, err := buildCreateTableOpFromSchema(media.Schema, schemaName)
		if err != nil {
			return nil, fmt.Errorf("build create table for %s: %w", opInfo.path, err)
		}
		plan.Add(createTable)
		generatedTables[schemaName] = true
	}

	fetchedResponseValues := make(map[string][]interface{})

	for {
		shouldContinue := false

		paths := make([]string, 0, len(operationDependencies))
		for p := range operationDependencies {
			paths = append(paths, p)
		}
		sort.Strings(paths)

		for _, path := range paths {
			opInfo := operationDependencies[path]
			if opInfo.IsFetched {
				continue
			}

			var combinations [][]interface{}
			if len(opInfo.ParamSpecs) == 0 {
				combinations = [][]interface{}{{}}
			} else {
				valueLists, ready, err := s.buildEffectiveParamValueLists(path, opInfo.ParamSpecs, fetchedResponseValues)
				if err != nil {
					return nil, err
				}
				if !ready {
					continue
				}
				combinations = s.generateCombinations(valueLists)
			}

			if len(combinations) > 0 {
				for _, combo := range combinations {
					params := s.buildParamsMapFromSpecs(opInfo.ParamSpecs, combo)
					params, missingAuth := s.applyAuthParams(path, params, opInfo.AuthSpecs)
					if missingAuth {
						continue
					}
					insertOps, err := s.fetchAndBuildInsertOps(ctx, baseURL, path, opInfo.Op, params, opInfo.PaginationSpec, fetchedResponseValues, opInfo.Plan, opInfo.AuthSpecs)
					if err != nil {
						log.Printf("failed to build INSERT ops for GET %s: %v", path, err)
						continue
					}
					for _, op := range insertOps {
						plan.Add(op)
					}
				}
				shouldContinue = true
			}

			opInfo.IsFetched = true
		}

		if !shouldContinue {
			break
		}
	}

	return plan, nil
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

func (s *Service) fetchAndBuildInsertOps(ctx context.Context, baseURL, path string, op *openapi3.Operation, params map[string]string, paginationSpec *ParamDataSpec, fetchedResponseValues map[string][]interface{}, plan *operationExtractionPlan, authSpecs []AuthParamSpec) ([]MigrationOperation, error) {
	resp := s.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return nil, fmt.Errorf("no successful response found")
	}

	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return nil, fmt.Errorf("no JSON response schema found")
	}

	allInsertOps := make([]MigrationOperation, 0)
	paginationValue, hasPaginationValue := initialPaginationValue(paginationSpec)

	for {
		requestParams := cloneStringMap(params)
		if paginationSpec != nil && hasPaginationValue {
			requestParams[paginationSpec.ParamName] = fmt.Sprintf("%v", paginationValue)
		}

		request, err := buildFetchRequest(baseURL, path, op, requestParams, authSpecs)
		if err != nil {
			return nil, err
		}
		result, err := s.api.Fetch(ctx, request)
		if err != nil {
			log.Printf("warning: failed to fetch data for %s: %v", path, err)
			return nil, nil
		}

		s.extractResponseDataFromData(result.Payload, media, fetchedResponseValues)
		s.extractResponseDataFromMarkedData(result.Payload, media.Schema, fetchedResponseValues)

		pageInsertOps, hasRows, err := s.buildInsertOpsForPayload(result.Payload, resp, media, plan)
		if err != nil {
			return nil, err
		}
		allInsertOps = append(allInsertOps, pageInsertOps...)

		if paginationSpec == nil {
			break
		}

		switch paginationSpec.Type {
		case paramDataTypeCursor:
			nextValue, ok := extractCursorValue(result.Payload, paginationSpec.CursorPath)
			if !ok {
				return allInsertOps, nil
			}
			paginationValue = nextValue
			hasPaginationValue = true
		case paramDataTypeOffset:
			if !hasRows {
				return allInsertOps, nil
			}
			nextValue, err := incrementOffsetValue(paginationValue, paginationSpec.OffsetConfig.Increment)
			if err != nil {
				return nil, fmt.Errorf("%s parameter %s: %w", path, paginationSpec.ParamName, err)
			}
			paginationValue = nextValue
			hasPaginationValue = true
		default:
			return nil, fmt.Errorf("unsupported pagination type %q", paginationSpec.Type)
		}
	}

	return allInsertOps, nil
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

func (s *Service) collectOperationsWithResType(spec *openapi3.T) []struct {
	path string
	op   *openapi3.Operation
} {
	var operations []struct {
		path string
		op   *openapi3.Operation
	}
	for path, pathItem := range spec.Paths.Map() {
		if pathItem == nil || pathItem.Get == nil {
			continue
		}
		if _, ok := pathItem.Get.Extensions["x-res-type"]; ok {
			operations = append(operations, struct {
				path string
				op   *openapi3.Operation
			}{path: path, op: pathItem.Get})
		}
	}
	sort.Slice(operations, func(i, j int) bool { return operations[i].path < operations[j].path })
	return operations
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
