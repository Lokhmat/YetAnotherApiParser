package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

type FKFilterSpec struct {
	Op     string
	Value  interface{}
	Values []interface{}
}

type FKParamSpec struct {
	ParamName     string
	DependencyKey string
	SeedValues    []interface{}
	Filter        *FKFilterSpec
}

type operationInfo struct {
	Path      string
	Op        *openapi3.Operation
	FKSpecs   []FKParamSpec
	IsFetched bool
	Plan      *operationExtractionPlan
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
		fkSpecs, err := s.getFKParamSpecs(opInfo.path, opInfo.op)
		if err != nil {
			return nil, fmt.Errorf("parse x-fk for %s: %w", opInfo.path, err)
		}
		extractionPlan, err := s.buildOperationExtractionPlan(opInfo.op)
		if err != nil {
			return nil, fmt.Errorf("build extraction plan for %s: %w", opInfo.path, err)
		}

		operationDependencies[opInfo.path] = &operationInfo{
			Path:      opInfo.path,
			Op:        opInfo.op,
			FKSpecs:   fkSpecs,
			Plan:      extractionPlan,
			IsFetched: false,
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

	fetchedFKValues := make(map[string][]interface{})

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
			if len(opInfo.FKSpecs) == 0 {
				combinations = [][]interface{}{{}}
			} else {
				fkValueLists, ready, err := s.buildEffectiveFKValueLists(path, opInfo.FKSpecs, fetchedFKValues)
				if err != nil {
					return nil, err
				}
				if !ready {
					continue
				}
				combinations = s.generateCombinations(fkValueLists)
			}

			if len(combinations) > 0 {
				for _, combo := range combinations {
					params := s.buildParamsMapFromSpecs(opInfo.FKSpecs, combo)
					insertOps, err := s.fetchAndBuildInsertOps(ctx, baseURL, path, opInfo.Op, params, fetchedFKValues, opInfo.Plan)
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

func (s *Service) fetchAndBuildInsertOps(ctx context.Context, baseURL, path string, op *openapi3.Operation, params map[string]string, fetchedFKValues map[string][]interface{}, plan *operationExtractionPlan) ([]MigrationOperation, error) {
	resp := s.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return nil, fmt.Errorf("no successful response found")
	}

	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return nil, fmt.Errorf("no JSON response schema found")
	}

	request, err := buildFetchRequest(baseURL, path, op, params)
	if err != nil {
		return nil, err
	}
	result, err := s.api.Fetch(ctx, request)
	if err != nil {
		log.Printf("warning: failed to fetch data for %s: %v", path, err)
		return nil, nil
	}

	s.extractFKValuesFromData(result.Payload, op, media, fetchedFKValues)
	s.extractFKValuesFromMarkedData(result.Payload, media.Schema, fetchedFKValues)

	if plan != nil && plan.HasMarks {
		insertOps, err := s.buildInsertOpsFromMarkedPlan(result.Payload, plan)
		if err != nil {
			log.Printf("warning: failed to build marked INSERT operations: %v", err)
			return nil, nil
		}
		return insertOps, nil
	}

	schemaName := s.getSchemaName(media.Schema)
	expectedColumns := s.getExpectedColumns(resp)
	insertOp, err := s.buildInsertRowsOp(result.Payload, schemaName, expectedColumns)
	if err != nil {
		log.Printf("warning: failed to build INSERT operations: %v", err)
		return nil, nil
	}
	if len(insertOp.Rows) == 0 {
		return nil, nil
	}
	return []MigrationOperation{insertOp}, nil
}

func buildFetchRequest(baseURL, path string, op *openapi3.Operation, params map[string]string) (FetchRequest, error) {
	req := FetchRequest{
		Method:      "GET",
		BaseURL:     baseURL,
		Path:        path,
		PathParams:  map[string]string{},
		QueryParams: map[string]string{},
		Headers:     map[string]string{},
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

func (s *Service) getFKParamSpecs(opPath string, op *openapi3.Operation) ([]FKParamSpec, error) {
	var specs []FKParamSpec
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		raw, ok := paramRef.Value.Extensions["x-fk"]
		if !ok {
			continue
		}
		spec, err := parseFKParamSpec(raw, paramRef.Value.Name)
		if err != nil {
			return nil, fmt.Errorf("%s parameter %s: %w", opPath, paramRef.Value.Name, err)
		}
		if err := validateFKSpec(spec); err != nil {
			return nil, fmt.Errorf("%s parameter %s: %w", opPath, paramRef.Value.Name, err)
		}
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].ParamName < specs[j].ParamName })
	return specs, nil
}

func (s *Service) buildParamsMapFromSpecs(specs []FKParamSpec, values []interface{}) map[string]string {
	params := make(map[string]string)
	for i, fk := range specs {
		params[fk.ParamName] = fmt.Sprintf("%v", values[i])
	}
	return params
}

func (s *Service) buildEffectiveFKValueLists(opPath string, specs []FKParamSpec, fetchedFKValues map[string][]interface{}) ([][]interface{}, bool, error) {
	fkValueLists := make([][]interface{}, 0, len(specs))
	for _, fk := range specs {
		values := make([]interface{}, 0, len(fk.SeedValues))
		values = append(values, fk.SeedValues...)
		if fetchedValues, ok := fetchedFKValues[fk.DependencyKey]; ok {
			values = append(values, fetchedValues...)
		}
		values = uniqueValues(values)

		filtered, err := applyFKFilter(values, fk.Filter)
		if err != nil {
			return nil, false, fmt.Errorf("%s parameter %s: %w", opPath, fk.ParamName, err)
		}
		if len(filtered) == 0 {
			return nil, false, nil
		}
		fkValueLists = append(fkValueLists, filtered)
	}
	return fkValueLists, true, nil
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

func (s *Service) findFKFields(op *openapi3.Operation, media *openapi3.MediaType) map[string]string {
	fkFieldNames := make(map[string]string)
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		if raw, ok := paramRef.Value.Extensions["x-fk"]; ok {
			fkSpec, err := parseFKParamSpec(raw, paramRef.Value.Name)
			if err != nil {
				continue
			}
			fkFieldNames[fkSpec.DependencyKey] = fkSpec.DependencyKey
		}
	}

	schema := media.Schema.Value
	if schema == nil {
		return fkFieldNames
	}
	if schema.Type != nil && schema.Type.Is("array") && schema.Items != nil {
		schema = schema.Items.Value
	}
	if schema != nil && schema.Properties != nil {
		for propName, propRef := range schema.Properties {
			if propRef == nil || propRef.Value == nil {
				continue
			}
			if fkVal, ok := propRef.Value.Extensions["x-fk"]; ok {
				dependencyKey := fmt.Sprintf("%v", fkVal)
				if dependencyKey != "" {
					fkFieldNames[dependencyKey] = propName
				}
			}
		}
	}
	return fkFieldNames
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

func (s *Service) extractFKValuesFromData(data []byte, op *openapi3.Operation, media *openapi3.MediaType, fkValues map[string][]interface{}) {
	if media == nil || media.Schema == nil {
		return
	}
	records, err := parseJSONRecords(data)
	if err != nil {
		return
	}
	fkFieldNames := s.findFKFields(op, media)
	for _, record := range records {
		for dependencyKey, responseField := range fkFieldNames {
			if val, exists := record[responseField]; exists {
				fkValues[dependencyKey] = appendUniqueValue(fkValues[dependencyKey], val)
			}
		}
	}
}

func (s *Service) extractFKValuesFromMarkedData(data []byte, schemaRef *openapi3.SchemaRef, fkValues map[string][]interface{}) {
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
				if fkVal, ok := propRef.Value.Extensions["x-fk"]; ok {
					depKey := fmt.Sprintf("%v", fkVal)
					if depKey != "" {
						fkValues[depKey] = appendUniqueValue(fkValues[depKey], propVal)
					}
				}
				walk(propRef.Value, propVal)
			}
		}
	}
	walk(schemaRef.Value, payload)
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

func parseFKParamSpec(raw interface{}, paramName string) (FKParamSpec, error) {
	spec := FKParamSpec{ParamName: paramName, DependencyKey: paramName}
	switch v := raw.(type) {
	case bool:
		if !v {
			return spec, fmt.Errorf("x-fk boolean value must be true")
		}
		return spec, nil
	case map[string]any:
		return parseFKParamSpecMap(spec, map[string]interface{}(v))
	case string:
		return spec, fmt.Errorf("x-fk string format is deprecated; use x-fk object with id")
	default:
		return spec, fmt.Errorf("unsupported x-fk type: %T", raw)
	}
}

func parseFKParamSpecMap(spec FKParamSpec, m map[string]interface{}) (FKParamSpec, error) {
	if idRaw, ok := m["id"]; ok {
		id, ok := idRaw.(string)
		if !ok || strings.TrimSpace(id) == "" {
			return spec, fmt.Errorf("x-fk.id must be non-empty string")
		}
		spec.DependencyKey = strings.TrimSpace(id)
	}
	if valuesRaw, ok := m["values"]; ok {
		values, err := parseFilterValuesArray(valuesRaw, "x-fk.values")
		if err != nil {
			return spec, err
		}
		spec.SeedValues = uniqueValues(values)
	}
	if filterRaw, ok := m["filter"]; ok {
		filter, err := parseFilterSpec(filterRaw)
		if err != nil {
			return spec, err
		}
		spec.Filter = filter
	}
	return spec, nil
}

func validateFKSpec(spec FKParamSpec) error {
	if strings.TrimSpace(spec.ParamName) == "" {
		return fmt.Errorf("parameter name is required")
	}
	if strings.TrimSpace(spec.DependencyKey) == "" {
		return fmt.Errorf("x-fk dependency key is required")
	}
	for _, v := range spec.SeedValues {
		if !isSupportedFKValue(v) {
			return fmt.Errorf("x-fk.values supports only scalar or array values")
		}
	}
	return nil
}

func parseFilterSpec(raw interface{}) (*FKFilterSpec, error) {
	filterMap, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("x-fk.filter must be object")
	}
	opRaw, ok := filterMap["op"]
	if !ok {
		return nil, fmt.Errorf("x-fk.filter.op is required")
	}
	op, ok := opRaw.(string)
	if !ok || strings.TrimSpace(op) == "" {
		return nil, fmt.Errorf("x-fk.filter.op must be non-empty string")
	}
	op = strings.ToLower(strings.TrimSpace(op))

	spec := &FKFilterSpec{Op: op}
	switch op {
	case filterOpIn:
		valuesRaw, ok := filterMap["values"]
		if !ok {
			return nil, fmt.Errorf("x-fk.filter.values is required for 'in'")
		}
		values, err := parseFilterValuesArray(valuesRaw, "x-fk.filter.values")
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("x-fk.filter.values must be non-empty")
		}
		if _, hasValue := filterMap["value"]; hasValue {
			return nil, fmt.Errorf("x-fk.filter.value is not allowed for 'in'")
		}
		spec.Values = uniqueValues(values)
	case filterOpGT, filterOpGTE, filterOpLT, filterOpLTE:
		valueRaw, ok := filterMap["value"]
		if !ok {
			return nil, fmt.Errorf("x-fk.filter.value is required for '%s'", op)
		}
		if !isScalarValue(valueRaw) {
			return nil, fmt.Errorf("x-fk.filter.value must be scalar")
		}
		if _, hasValues := filterMap["values"]; hasValues {
			return nil, fmt.Errorf("x-fk.filter.values is not allowed for '%s'", op)
		}
		if _, ok := toNumber(valueRaw); !ok {
			if _, ok := toRFC3339Time(valueRaw); !ok {
				return nil, fmt.Errorf("x-fk.filter.value for '%s' must be numeric or RFC3339 datetime string", op)
			}
		}
		spec.Value = valueRaw
	default:
		return nil, fmt.Errorf("unsupported x-fk.filter.op: %s", op)
	}
	return spec, nil
}

func applyFKFilter(values []interface{}, filter *FKFilterSpec) ([]interface{}, error) {
	if filter == nil {
		return values, nil
	}
	filtered := make([]interface{}, 0, len(values))
	for _, candidate := range values {
		ok, err := matchFKFilter(candidate, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			filtered = append(filtered, candidate)
		}
	}
	return filtered, nil
}

func matchFKFilter(candidate interface{}, filter *FKFilterSpec) (bool, error) {
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
		if !isSupportedFKValue(item) {
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

func isSupportedFKValue(v interface{}) bool {
	if isScalarValue(v) {
		return true
	}
	arr, ok := v.([]interface{})
	if !ok {
		return false
	}
	for _, item := range arr {
		if !isSupportedFKValue(item) {
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
