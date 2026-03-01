package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"

	"api-parser/internal/ddl"
	"api-parser/internal/fetcher"
	"github.com/getkin/kin-openapi/openapi3"
)

type MigrationGenerator struct {
	fetcher *fetcher.Fetcher
	maxRPM  int
}

func New(maxRPM int, fetcherCfgs ...fetcher.ClientConfig) *MigrationGenerator {
	var fetcherCfg fetcher.ClientConfig
	if len(fetcherCfgs) > 0 {
		fetcherCfg = fetcherCfgs[0]
	}
	return &MigrationGenerator{
		fetcher: fetcher.New(maxRPM, fetcherCfg),
		maxRPM:  maxRPM,
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

// OperationInfo holds information about an operation with its x-fk dependencies
type OperationInfo struct {
	Path      string
	Op        *openapi3.Operation
	FKSpecs   []FKParamSpec
	IsFetched bool // whether this operation has been fetched (with all required FK values)
	Plan      *OperationExtractionPlan
}

type RelationColumnPlan struct {
	Name     string
	Type     string
	IsArray  bool
	ElemType string
}

type MarkedNodePlan struct {
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
	RelationColumns map[string]RelationColumnPlan
}

type RelationPlan struct {
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

	// direct ref fields
	RefSQLType string
	IsArrayRef bool

	// link table fields
	JoinTableName  string
	JoinParentCol  string
	JoinParentType string
	JoinChildCol   string
	JoinChildType  string
}

type OperationExtractionPlan struct {
	HasMarks       bool
	Nodes          []*MarkedNodePlan
	NodeByPath     map[string]*MarkedNodePlan
	ChildRelations map[string][]*RelationPlan
	Relations      []*RelationPlan
}

var sqlIdentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// GenerateMigrations generates migration DDL statements from OpenAPI specification
func (m *MigrationGenerator) GenerateMigrations(ctx context.Context, spec *openapi3.T, baseURL string) ([]string, error) {
	var migrations []string

	// 1. Get all operations with x-res-type and build operation dependencies map
	// operationDependencies: operation path -> operation info (x-fk dependencies, fetch status)
	operationDependencies := make(map[string]*OperationInfo)

	// generatedTables: tracks which table names have already been created
	generatedTables := make(map[string]bool)

	operations := m.collectOperationsWithResType(spec)
	for _, opInfo := range operations {
		path := opInfo.path
		op := opInfo.op

		fkSpecs, err := m.getFKParamSpecs(path, op)
		if err != nil {
			return nil, fmt.Errorf("parse x-fk for %s: %w", path, err)
		}
		plan, err := m.buildOperationExtractionPlan(op)
		if err != nil {
			return nil, fmt.Errorf("build extraction plan for %s: %w", path, err)
		}

		operationDependencies[path] = &OperationInfo{
			Path:      path,
			Op:        op,
			FKSpecs:   fkSpecs,
			IsFetched: false,
			Plan:      plan,
		}
	}

	// 2. Create map B: FK name -> slice of all fetched values
	// fetchedFKValues: tracks all FK values we've collected so far
	fetchedFKValues := make(map[string][]interface{})

	// 3. Start infinite loop - fetch operations when all their dependencies are met
	for {
		shouldContinue := false

		paths := make([]string, 0, len(operationDependencies))
		for p := range operationDependencies {
			paths = append(paths, p)
		}
		sort.Strings(paths)

		// 4. Iterate through all operations
		for _, path := range paths {
			opInfo := operationDependencies[path]
			// Skip if already fetched
			if opInfo.IsFetched {
				continue
			}

			var combinations [][]interface{}
			if len(opInfo.FKSpecs) == 0 {
				combinations = [][]interface{}{{}}
			} else {
				fkValueLists, ready, err := m.buildEffectiveFKValueLists(path, opInfo.FKSpecs, fetchedFKValues)
				if err != nil {
					return nil, err
				}
				if !ready {
					opInfo.IsFetched = true
					continue
				}
				combinations = m.generateCombinations(fkValueLists)
			}

			if combinations != nil && len(combinations) > 0 {
				if opInfo.Plan != nil && opInfo.Plan.HasMarks {
					for _, node := range opInfo.Plan.Nodes {
						if generatedTables[node.TableName] {
							continue
						}
						relationCols := m.getRelationColumnsForNode(opInfo.Plan, node.Path)
						ddlStr, err := ddl.GenerateDDLFromObjectSchema(node.Schema, node.TableName, relationCols)
						if err != nil {
							return nil, fmt.Errorf("generate marked DDL for %s: %w", node.TableName, err)
						}
						migrations = append(migrations, ddlStr)
						generatedTables[node.TableName] = true
					}
					for _, rel := range opInfo.Plan.Relations {
						if rel.Kind != relationLinkTable {
							continue
						}
						if generatedTables[rel.JoinTableName] {
							continue
						}
						migrations = append(migrations, ddl.GenerateJoinTableDDL(
							rel.JoinTableName,
							rel.JoinParentCol,
							rel.JoinParentType,
							rel.JoinChildCol,
							rel.JoinChildType,
						))
						generatedTables[rel.JoinTableName] = true
					}
				} else {
					resp := m.getSuccessResponse(opInfo.Op)
					var schemaName string
					if resp != nil && resp.Value != nil {
						media := resp.Value.Content.Get("application/json")
						if media != nil && media.Schema != nil {
							schemaName = m.getSchemaName(media.Schema)
						}
					}

					// Generate CREATE TABLE only once per unique table
					if schemaName != "" && !generatedTables[schemaName] {
						media := resp.Value.Content.Get("application/json")
						if media == nil || media.Schema == nil {
							log.Printf("Failed to generate CREATE TABLE for %s: missing JSON schema", path)
							continue
						}
						ddlStr, err := ddl.GenerateDDLFromSchema(
							media.Schema,
							spec,
							schemaName,
						)
						if err != nil {
							log.Printf("Failed to generate CREATE TABLE for %s: %v", path, err)
						} else if ddlStr != "" {
							migrations = append(migrations, ddlStr)
							generatedTables[schemaName] = true
						}
					}
				}

				// Fetch data and generate INSERT statements for each combination
				for _, combo := range combinations {
					params := m.buildParamsMapFromSpecs(opInfo.FKSpecs, combo)
					insertSQL, err := m.FetchDataAndGenerateInserts(ctx, baseURL, path, opInfo.Op, params, spec, fetchedFKValues, opInfo.Plan)
					if err != nil {
						log.Printf("Failed to generate INSERT for %s %s: %v", "GET", path, err)
						continue
					}
					if insertSQL != "" {
						migrations = append(migrations, insertSQL)
					}
				}
				shouldContinue = true
			}

			// Mark as fetched
			opInfo.IsFetched = true
		}

		// 5. If no operations were fetched in this iteration, break
		if !shouldContinue {
			break
		}

	}

	return migrations, nil
}

func (m *MigrationGenerator) getSchemaName(schemaRef *openapi3.SchemaRef) string {
	schemaName := "response"
	if schemaRef != nil && schemaRef.Ref != "" {
		parts := strings.Split(schemaRef.Ref, "/")
		if len(parts) > 0 {
			schemaName = parts[len(parts)-1]
		}
	}
	return schemaName
}

// collectOperationsWithResType returns all GET operations with x-res-type extension
func (m *MigrationGenerator) collectOperationsWithResType(spec *openapi3.T) []struct {
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

		op := pathItem.Get
		if _, ok := op.Extensions["x-res-type"]; ok {
			operations = append(operations, struct {
				path string
				op   *openapi3.Operation
			}{path: path, op: op})
		}
	}

	sort.Slice(operations, func(i, j int) bool { return operations[i].path < operations[j].path })
	return operations
}

// getFKParamSpecs returns normalized x-fk parameter configs for an operation.
func (m *MigrationGenerator) getFKParamSpecs(opPath string, op *openapi3.Operation) ([]FKParamSpec, error) {
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

// buildParamsMapFromSpecs builds a params map from FK specs and values.
func (m *MigrationGenerator) buildParamsMapFromSpecs(specs []FKParamSpec, values []interface{}) map[string]string {
	params := make(map[string]string)
	for i, fk := range specs {
		params[fk.ParamName] = fmt.Sprintf("%v", values[i])
	}
	return params
}

func (m *MigrationGenerator) buildEffectiveFKValueLists(opPath string, specs []FKParamSpec, fetchedFKValues map[string][]interface{}) ([][]interface{}, bool, error) {
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

// generateCombinations generates all combinations from multiple value lists
func (m *MigrationGenerator) generateCombinations(valueLists [][]interface{}) [][]interface{} {
	if len(valueLists) == 0 {
		return nil
	}

	// Start with the first list
	result := make([][]interface{}, 0)
	for _, v := range valueLists[0] {
		result = append(result, []interface{}{v})
	}

	// For each subsequent list, combine with existing results
	for i := 1; i < len(valueLists); i++ {
		newResult := make([][]interface{}, 0)
		for _, combo := range result {
			for _, v := range valueLists[i] {
				newCombo := make([]interface{}, len(combo)+1)
				copy(newCombo, combo)
				newCombo[len(combo)] = v
				newResult = append(newResult, newCombo)
			}
		}
		result = newResult
	}

	return result
}

// findFKFields finds fields with x-fk extension in the operation parameters and response schema
func (m *MigrationGenerator) findFKFields(op *openapi3.Operation, media *openapi3.MediaType, spec *openapi3.T) map[string]string {
	fkFieldNames := make(map[string]string) // map of parameter name -> response field name

	// First, check parameter extensions for x-fk
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		if raw, ok := paramRef.Value.Extensions["x-fk"]; ok {
			fkSpec, err := parseFKParamSpec(raw, paramRef.Value.Name)
			if err != nil {
				continue
			}
			// For parameter-level x-fk mapping we read from dependency key field in response.
			fkFieldNames[fkSpec.DependencyKey] = fkSpec.DependencyKey
		}
	}

	schema := media.Schema.Value
	if schema == nil {
		return fkFieldNames
	}

	// Handle array type - get the items schema
	if schema.Type != nil && schema.Type.Is("array") && schema.Items != nil {
		schema = schema.Items.Value
	}

	// Check schema properties for x-fk extensions.
	// x-fk value is the dependency key; property name is where to read it from response JSON.
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

// getExpectedColumns extracts column names from the response schema
func (m *MigrationGenerator) getExpectedColumns(resp *openapi3.ResponseRef, spec *openapi3.T) []string {
	if resp == nil || resp.Value == nil {
		return nil
	}

	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return nil
	}

	// Start with the schema
	schema := media.Schema.Value
	if schema == nil {
		return nil
	}

	// Handle array type - get the items schema
	if schema.Type != nil && schema.Type.Is("array") && schema.Items != nil {
		schema = schema.Items.Value
	}

	// Extract column names from schema properties
	var columns []string
	if schema != nil && schema.Properties != nil {
		for propName := range schema.Properties {
			columns = append(columns, propName)
		}
	}
	sort.Strings(columns)
	return columns
}

// generateInsertStatements generates INSERT statements from JSON data
// Only includes columns that are expected (defined in schema) to avoid unexpected fields from API
func (m *MigrationGenerator) generateInsertStatements(data []byte, tableName string, expectedColumns []string) (string, error) {
	records, err := parseJSONRecords(data)
	if err != nil {
		return "", fmt.Errorf("unmarshal JSON: %w", err)
	}

	var b strings.Builder
	for _, record := range records {
		var columns []string
		var values []string

		// Only include columns that are expected (defined in schema)
		for _, colName := range expectedColumns {
			if value, exists := record[colName]; exists {
				columns = append(columns, colName)
				values = append(values, toSQLLiteral(value))
			}
		}

		if len(columns) > 0 {
			insertStmt := fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s);",
				tableName,
				strings.Join(columns, ", "),
				strings.Join(values, ", "),
			)
			b.WriteString(insertStmt)
			b.WriteString("\n")
		}
	}

	return b.String(), nil
}

// FetchDataAndGenerateInserts fetches data and generates INSERT statements.
func (m *MigrationGenerator) FetchDataAndGenerateInserts(ctx context.Context, baseURL, path string, op *openapi3.Operation, params map[string]string, spec *openapi3.T, fetchedFKValues map[string][]interface{}, plan *OperationExtractionPlan) (string, error) {
	resp := m.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return "", fmt.Errorf("no successful response found")
	}

	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return "", fmt.Errorf("no JSON response schema found")
	}

	data, fetchErr := m.fetcher.FetchData(ctx, baseURL, path, op, params)
	if fetchErr != nil {
		log.Printf("Warning: failed to fetch data for %s: %v", path, fetchErr)
		return "", nil
	}

	m.extractFKValuesFromData(data, op, media, spec, fetchedFKValues)
	m.extractFKValuesFromMarkedData(data, media.Schema, fetchedFKValues)

	if plan != nil && plan.HasMarks {
		insertStatements, err := m.generateInsertsFromMarkedPlan(data, plan)
		if err != nil {
			log.Printf("Warning: failed to generate marked INSERT statements: %v", err)
			return "", nil
		}
		return insertStatements, nil
	}

	// legacy path
	schemaName := m.getSchemaName(media.Schema)
	expectedColumns := m.getExpectedColumns(resp, spec)

	log.Printf("Successfully fetched data from %s", path)
	log.Printf("Data length: %d bytes", len(data))

	insertStatements, err := m.generateInsertStatements(data, schemaName, expectedColumns)
	if err != nil {
		log.Printf("Warning: failed to generate INSERT statements: %v", err)
		return "", nil
	}

	return insertStatements, nil
}

func (m *MigrationGenerator) extractFKValuesFromData(data []byte, op *openapi3.Operation, media *openapi3.MediaType, spec *openapi3.T, fkValues map[string][]interface{}) {
	if media == nil || media.Schema == nil {
		return
	}

	records, err := parseJSONRecords(data)
	if err != nil {
		return
	}

	fkFieldNames := m.findFKFields(op, media, spec)
	for _, record := range records {
		for dependencyKey, responseField := range fkFieldNames {
			if val, exists := record[responseField]; exists {
				fkValues[dependencyKey] = appendUniqueValue(fkValues[dependencyKey], val)
			}
		}
	}
}

func (m *MigrationGenerator) extractFKValuesFromMarkedData(data []byte, schemaRef *openapi3.SchemaRef, fkValues map[string][]interface{}) {
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

			propNames := make([]string, 0, len(schema.Properties))
			for name := range schema.Properties {
				propNames = append(propNames, name)
			}
			sort.Strings(propNames)

			for _, propName := range propNames {
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

func (m *MigrationGenerator) getSuccessResponse(op *openapi3.Operation) *openapi3.ResponseRef {
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

func toSQLLiteral(v interface{}) string {
	switch value := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(value, "'", "''") + "'"
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
		return "'" + strings.ReplaceAll(string(encoded), "'", "''") + "'"
	}
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
	spec := FKParamSpec{
		ParamName:     paramName,
		DependencyKey: paramName,
	}

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

func (m *MigrationGenerator) buildOperationExtractionPlan(op *openapi3.Operation) (*OperationExtractionPlan, error) {
	resp := m.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return &OperationExtractionPlan{HasMarks: false}, nil
	}
	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil || media.Schema.Value == nil {
		return &OperationExtractionPlan{HasMarks: false}, nil
	}

	plan := &OperationExtractionPlan{
		HasMarks:       false,
		Nodes:          []*MarkedNodePlan{},
		NodeByPath:     map[string]*MarkedNodePlan{},
		ChildRelations: map[string][]*RelationPlan{},
		Relations:      []*RelationPlan{},
	}
	tableSignatures := make(map[string]string)
	nodeCounter := 0

	var registerNode func(
		tableName string,
		nodeSchema *openapi3.Schema,
		nodeKind containerKind,
		accessPath []string,
		parentPath string,
		parentProp string,
		parentKind containerKind,
	) (*MarkedNodePlan, error)

	registerNode = func(tableName string, nodeSchema *openapi3.Schema, nodeKind containerKind, accessPath []string, parentPath string, parentProp string, parentKind containerKind) (*MarkedNodePlan, error) {
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
		node := &MarkedNodePlan{
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
			RelationColumns: map[string]RelationColumnPlan{},
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
			rel := &RelationPlan{
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
				if rel.IsArrayRef {
					rel.RefSQLType = node.PKSQLType + "[]"
				} else {
					rel.RefSQLType = node.PKSQLType
				}
				parentNode.RelationColumns[parentProp] = RelationColumnPlan{
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

	var walk func(schema *openapi3.Schema, accessPath []string, ownerNodePath string, ownerNodeKind containerKind, ownerProp string) error
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

func (m *MigrationGenerator) getRelationColumnsForNode(plan *OperationExtractionPlan, nodePath string) []ddl.RelationColumnSpec {
	node := plan.NodeByPath[nodePath]
	if node == nil || len(node.RelationColumns) == 0 {
		return nil
	}
	cols := make([]ddl.RelationColumnSpec, 0, len(node.RelationColumns))
	for _, c := range node.RelationColumns {
		cols = append(cols, ddl.RelationColumnSpec{Name: c.Name, Type: c.Type})
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
	plan           *OperationExtractionPlan
	insertedRows   map[string]map[string]bool
	insertedLinks  map[string]bool
	insertSQLLines []string
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

func toSQLArrayLiteral(values []interface{}, elemType string) string {
	cleanElemType := strings.TrimSuffix(elemType, "[]")
	if len(values) == 0 {
		return fmt.Sprintf("ARRAY[]::%s[]", cleanElemType)
	}
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, toSQLLiteral(v))
	}
	return fmt.Sprintf("ARRAY[%s]::%s[]", strings.Join(parts, ", "), cleanElemType)
}

func uniqueValues(values []interface{}) []interface{} {
	seen := map[string]bool{}
	result := make([]interface{}, 0, len(values))
	for _, v := range values {
		k := fmt.Sprintf("%v", v)
		if seen[k] {
			continue
		}
		seen[k] = true
		result = append(result, v)
	}
	return result
}

func (m *MigrationGenerator) generateInsertsFromMarkedPlan(data []byte, plan *OperationExtractionPlan) (string, error) {
	var payload interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("unmarshal JSON: %w", err)
	}

	ctx := &markedInsertContext{
		plan:           plan,
		insertedRows:   map[string]map[string]bool{},
		insertedLinks:  map[string]bool{},
		insertSQLLines: []string{},
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

	if len(ctx.insertSQLLines) == 0 {
		return "", nil
	}
	return strings.Join(ctx.insertSQLLines, "\n") + "\n", nil
}

func (c *markedInsertContext) processNode(node *MarkedNodePlan, raw interface{}) []interface{} {
	if node == nil || raw == nil {
		return nil
	}

	switch node.NodeKind {
	case containerObject:
		record, ok := raw.(map[string]interface{})
		if !ok {
			log.Printf("Warning: expected object for table %s", node.TableName)
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
				log.Printf("Warning: expected array for table %s", node.TableName)
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

func (c *markedInsertContext) processRecord(node *MarkedNodePlan, record map[string]interface{}) interface{} {
	if node == nil || record == nil {
		return nil
	}

	rowValues := make(map[string]interface{})
	for _, col := range node.ScalarColumns {
		if v, ok := record[col]; ok {
			rowValues[col] = v
		}
	}

	linkRows := make([]*RelationPlan, 0)
	linkChildValues := make(map[string][]interface{})
	rels := c.plan.ChildRelations[node.Path]
	for _, rel := range rels {
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
				rowValues[rel.ParentProp] = childPKs
			} else {
				rowValues[rel.ParentProp] = childPKs[0]
			}
		} else if rel.Kind == relationLinkTable {
			linkRows = append(linkRows, rel)
			linkChildValues[rel.ChildPath] = childPKs
		}
	}

	pk, ok := record[node.PKField]
	if !ok {
		log.Printf("Warning: missing x-pk field %s for table %s", node.PKField, node.TableName)
		return nil
	}
	pkKey := fmt.Sprintf("%v", pk)

	if c.recordInsertedRow(node.TableName, pkKey) {
		orderedColumns := make([]string, 0, len(node.ScalarColumns)+len(node.RelationColumns))
		for _, col := range node.ScalarColumns {
			orderedColumns = append(orderedColumns, col)
		}
		relNames := make([]string, 0, len(node.RelationColumns))
		for relCol := range node.RelationColumns {
			relNames = append(relNames, relCol)
		}
		sort.Strings(relNames)
		orderedColumns = append(orderedColumns, relNames...)

		cols := make([]string, 0, len(orderedColumns))
		vals := make([]string, 0, len(orderedColumns))
		for _, col := range orderedColumns {
			v, exists := rowValues[col]
			if !exists {
				continue
			}
			cols = append(cols, col)
			relCol, isRel := node.RelationColumns[col]
			if isRel && relCol.IsArray {
				arr, _ := v.([]interface{})
				vals = append(vals, toSQLArrayLiteral(arr, relCol.ElemType))
				continue
			}
			vals = append(vals, toSQLLiteral(v))
		}
		if len(cols) > 0 {
			c.insertSQLLines = append(c.insertSQLLines,
				fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", node.TableName, strings.Join(cols, ", "), strings.Join(vals, ", ")),
			)
		}
	}

	for _, rel := range linkRows {
		childPKs := uniqueValues(linkChildValues[rel.ChildPath])
		for _, childPK := range childPKs {
			linkKey := fmt.Sprintf("%s|%v|%v", rel.JoinTableName, pk, childPK)
			if c.insertedLinks[linkKey] {
				continue
			}
			c.insertedLinks[linkKey] = true
			c.insertSQLLines = append(c.insertSQLLines,
				fmt.Sprintf(
					"INSERT INTO %s (%s, %s) VALUES (%s, %s);",
					rel.JoinTableName,
					rel.JoinParentCol,
					rel.JoinChildCol,
					toSQLLiteral(pk),
					toSQLLiteral(childPK),
				),
			)
		}
	}

	return pk
}
