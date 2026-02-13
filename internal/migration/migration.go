package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"api-parser/internal/ddl"
	"api-parser/internal/fetcher"
	"github.com/getkin/kin-openapi/openapi3"
)

type MigrationGenerator struct {
	fetcher *fetcher.Fetcher
	maxRPM  int
}

func New(maxRPM int) *MigrationGenerator {
	return &MigrationGenerator{
		fetcher: fetcher.New(maxRPM),
		maxRPM:  maxRPM,
	}
}

// OperationInfo holds information about an operation with its x-fk dependencies
type OperationInfo struct {
	Path      string
	Op        *openapi3.Operation
	NeedFKs   []string // list of x-fk names needed by this operation
	IsFetched bool     // whether this operation has been fetched (with all required FK values)
}

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

		fkParamNames := m.getFKParamNames(op)
		operationDependencies[path] = &OperationInfo{
			Path:      path,
			Op:        op,
			NeedFKs:   fkParamNames,
			IsFetched: false,
		}
	}

	// 2. Create map B: FK name -> slice of all fetched values
	// fetchedFKValues: tracks all FK values we've collected so far
	fetchedFKValues := make(map[string][]interface{})

	// 3. Start infinite loop - fetch operations when all their dependencies are met
	for {
		shouldContinue := false

		// 4. Iterate through all operations
		for path, opInfo := range operationDependencies {
			// Skip if already fetched
			if opInfo.IsFetched {
				continue
			}

			// Check if all required FK values are available (or no FKs needed)
			if len(opInfo.NeedFKs) == 0 || m.hasAllFKValues(opInfo.NeedFKs, fetchedFKValues) {
				// Get all combinations of FK values if needed
				var combinations [][]interface{}
				if len(opInfo.NeedFKs) == 0 {
					combinations = [][]interface{}{{}} // No FKs needed - single empty combination
				} else {
					fkValueLists := make([][]interface{}, 0)
					for _, fkName := range opInfo.NeedFKs {
						if values, ok := fetchedFKValues[fkName]; ok {
							fkValueLists = append(fkValueLists, values)
						} else {
							fkValueLists = nil
							break
						}
					}
					if fkValueLists != nil && len(fkValueLists) > 0 {
						combinations = m.generateCombinations(fkValueLists)
					}
				}

				if combinations != nil && len(combinations) > 0 {
					// Get schema name for this operation (for table creation)
					resp := m.getSuccessResponse(opInfo.Op)
					var schemaName string
					if resp != nil && resp.Value != nil {
						media := resp.Value.Content.Get("application/json")
						if media != nil && media.Schema != nil {
							schemaName = "response"
							if media.Schema.Ref != "" {
								parts := strings.Split(media.Schema.Ref, "/")
								if len(parts) > 0 {
									schemaName = parts[len(parts)-1]
								}
							}
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

					// Fetch data and generate INSERT statements for each combination
					// Note: CREATE TABLE is already generated above, so we only need INSERT statements
					for _, combo := range combinations {
						params := m.buildParamsMap(opInfo.NeedFKs, combo)
						insertSQL, err := m.FetchDataAndGenerateInserts(ctx, baseURL, path, opInfo.Op, params, spec, fetchedFKValues)
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
		}

		// 5. If no operations were fetched in this iteration, break
		if !shouldContinue {
			break
		}

	}

	return migrations, nil
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

	return operations
}

// getFKParamNames returns list of parameter names that have x-fk extension
func (m *MigrationGenerator) getFKParamNames(op *openapi3.Operation) []string {
	var fkParamNames []string
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		if _, ok := paramRef.Value.Extensions["x-fk"]; ok {
			fkParamNames = append(fkParamNames, paramRef.Value.Name)
		}
	}
	return fkParamNames
}

// buildParamsMap builds a params map from FK names and values
func (m *MigrationGenerator) buildParamsMap(fkNames []string, values []interface{}) map[string]string {
	params := make(map[string]string)
	for i, fkName := range fkNames {
		params[fkName] = fmt.Sprintf("%v", values[i])
	}
	return params
}

// hasAllFKValues checks if all required FK names have values in fetchedFKValues
func (m *MigrationGenerator) hasAllFKValues(fkNames []string, fetchedFKValues map[string][]interface{}) bool {
	for _, fkName := range fkNames {
		if values, exists := fetchedFKValues[fkName]; !exists || len(values) == 0 {
			return false
		}
	}
	return true
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
		if fkVal, ok := paramRef.Value.Extensions["x-fk"]; ok {
			paramName := paramRef.Value.Name
			responseField := paramName
			if v, ok := fkVal.(string); ok && v != "" {
				responseField = v
			}
			fkFieldNames[paramName] = responseField
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

	return columns
}

func (m *MigrationGenerator) generateMigrationForOperation(ctx context.Context, baseURL, path string, op *openapi3.Operation, spec *openapi3.T) (string, error) {
	resp := m.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return "", fmt.Errorf("no successful response found")
	}

	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return "", fmt.Errorf("no JSON response schema found")
	}

	schemaName := "response"
	if media.Schema.Ref != "" {
		parts := strings.Split(media.Schema.Ref, "/")
		if len(parts) > 0 {
			schemaName = parts[len(parts)-1]
		}
	}

	ddlStr, err := ddl.GenerateDDLFromSchema(media.Schema, spec, schemaName)
	if err != nil {
		return "", fmt.Errorf("generate DDL: %w", err)
	}

	return ddlStr, nil
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

// FetchDataAndGenerateInserts fetches data and generates only INSERT statements (no CREATE TABLE)
// Used when generating migrations in dependency order - CREATE TABLE is generated once per table
func (m *MigrationGenerator) FetchDataAndGenerateInserts(ctx context.Context, baseURL, path string, op *openapi3.Operation, params map[string]string, spec *openapi3.T, fetchedFKValues map[string][]interface{}) (string, error) {
	resp := m.getSuccessResponse(op)
	if resp == nil || resp.Value == nil {
		return "", fmt.Errorf("no successful response found")
	}

	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return "", fmt.Errorf("no JSON response schema found")
	}

	// Extract schema name from response
	schemaName := "response"
	if media.Schema.Ref != "" {
		parts := strings.Split(media.Schema.Ref, "/")
		if len(parts) > 0 {
			schemaName = parts[len(parts)-1]
		}
	}

	// Get expected columns from the schema
	expectedColumns := m.getExpectedColumns(resp, spec)

	data, fetchErr := m.fetcher.FetchData(ctx, baseURL, path, op, params)
	if fetchErr != nil {
		log.Printf("Warning: failed to fetch data for %s: %v", path, fetchErr)
		return "", nil
	}

	m.extractFKValuesFromData(data, op, media, spec, fetchedFKValues)

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
