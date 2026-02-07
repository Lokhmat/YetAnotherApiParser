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
}

func New() *MigrationGenerator {
	return &MigrationGenerator{
		fetcher: fetcher.New(),
	}
}

// OperationInfo holds information about an operation with its x-fk dependencies
type OperationInfo struct {
	Path       string
	Op         *openapi3.Operation
	NeedFKs    []string // list of x-fk names needed by this operation
	IsFetched  bool     // whether this operation has been fetched (with all required FK values)
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
					resp := opInfo.Op.Responses.Value("200")
					if resp == nil {
						resp = opInfo.Op.Responses.Value("201")
					}
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
						ddlStr, err := ddl.GenerateDDLFromSchema(
							opInfo.Op.Responses.Value("200").Value.Content.Get("application/json").Schema,
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
						insertSQL, err := m.FetchDataAndGenerateInserts(ctx, baseURL, path, opInfo.Op, params, spec)
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

		// 6. Extract new FK values from the latest fetches
		m.extractFKValuesFromOperations(ctx, baseURL, operations, spec, fetchedFKValues)
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

// extractFKValuesFromOperations extracts FK values from operations that have been fetched
func (m *MigrationGenerator) extractFKValuesFromOperations(ctx context.Context, baseURL string, operations []struct {
	path string
	op   *openapi3.Operation
}, spec *openapi3.T, fetchedFKValues map[string][]interface{}) {
	// For each operation that was just fetched, extract new FK values
	// Only extract from operations that have no FK dependencies (source operations)
	for _, opInfo := range operations {
		// Only extract FK values from operations without dependencies
		// because those with dependencies are fetched in the loop with params
		if len(opInfo.op.Parameters) == 0 {
			m.extractFKValuesFromResponse(ctx, baseURL, opInfo.path, opInfo.op, spec, fetchedFKValues)
		}
	}
}

// extractFKValuesFromResponse fetches the data and extracts x-fk values from the response
func (m *MigrationGenerator) extractFKValuesFromResponse(ctx context.Context, baseURL, path string, op *openapi3.Operation, spec *openapi3.T, fkValues map[string][]interface{}) {
	// Get response schema first
	resp := op.Responses.Value("200")
	if resp == nil {
		resp = op.Responses.Value("201")
	}
	if resp == nil || resp.Value == nil {
		return
	}

	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil {
		return
	}

	// Try to fetch data
	data, err := m.fetcher.FetchData(ctx, baseURL, path, op, nil)
	if err != nil {
		log.Printf("Warning: failed to fetch data for %s: %v", path, err)
		return
	}

	// Parse the JSON and extract x-fk values from response
	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		log.Printf("Warning: failed to unmarshal data for %s: %v", path, err)
		return
	}

	// Find fields with x-fk extension
	fkFieldNames := m.findFKFields(op, media, spec)

	// Extract FK values from each record
	for _, record := range records {
		for paramName, responseField := range fkFieldNames {
			if val, exists := record[responseField]; exists {
				fkValues[paramName] = append(fkValues[paramName], val)
			}
		}
	}
}

// findFKFields finds fields with x-fk extension in the operation parameters and response schema
func (m *MigrationGenerator) findFKFields(op *openapi3.Operation, media *openapi3.MediaType, spec *openapi3.T) map[string]string {
	fkFieldNames := make(map[string]string) // map of parameter name -> response field name

	// First, check parameter extensions for x-fk
	for _, paramRef := range op.Parameters {
		if paramRef == nil || paramRef.Value == nil {
			continue
		}
		if _, ok := paramRef.Value.Extensions["x-fk"]; ok {
			paramName := paramRef.Value.Name
			responseField := paramName
			fkFieldNames[paramName] = responseField
		}
	}

	// If no FK field names found from parameters, try to find them from the schema
	if len(fkFieldNames) == 0 {
		schema := media.Schema.Value
		if schema == nil {
			return fkFieldNames
		}

		// Handle array type - get the items schema
		if schema.Type != nil && schema.Type.Is("array") && schema.Items != nil {
			schema = schema.Items.Value
		}

		// Check schema properties for x-fk extensions
		if schema != nil && schema.Properties != nil {
			for propName, propRef := range schema.Properties {
				if propRef == nil || propRef.Value == nil {
					continue
				}
				if fkVal, ok := propRef.Value.Extensions["x-fk"]; ok {
					responseField := propName
					paramName := fmt.Sprintf("%v", fkVal)
					fkFieldNames[paramName] = responseField
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
	resp := op.Responses.Value("200")
	if resp == nil {
		resp = op.Responses.Value("201")
	}
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
	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		return "", fmt.Errorf("unmarshal JSON: %w", err)
	}

	var insertStatements string
	for _, record := range records {
		var columns []string
		var values []string

		// Only include columns that are expected (defined in schema)
		for _, colName := range expectedColumns {
			if value, exists := record[colName]; exists {
				columns = append(columns, colName)
				strVal, ok := value.(string)
				if ok {
					escaped := strings.ReplaceAll(strVal, "'", "''")
					values = append(values, fmt.Sprintf("'%s'", escaped))
				} else {
					values = append(values, fmt.Sprintf("%v", value))
				}
			}
		}

		if len(columns) > 0 {
			insertStmt := fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s);",
				tableName,
				strings.Join(columns, ", "),
				strings.Join(values, ", "),
			)
			insertStatements += insertStmt + "\n"
		}
	}

	return insertStatements, nil
}

// FetchDataAndGenerateInserts fetches data and generates only INSERT statements (no CREATE TABLE)
// Used when generating migrations in dependency order - CREATE TABLE is generated once per table
func (m *MigrationGenerator) FetchDataAndGenerateInserts(ctx context.Context, baseURL, path string, op *openapi3.Operation, params map[string]string, spec *openapi3.T) (string, error) {
	resp := op.Responses.Value("200")
	if resp == nil {
		resp = op.Responses.Value("201")
	}
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

	log.Printf("Successfully fetched data from %s", path)
	log.Printf("Data length: %d bytes", len(data))

	insertStatements, err := m.generateInsertStatements(data, schemaName, expectedColumns)
	if err != nil {
		log.Printf("Warning: failed to generate INSERT statements: %v", err)
		return "", nil
	}

	return insertStatements, nil
}
