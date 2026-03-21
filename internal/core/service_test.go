package core

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

type fakeAPIConnector struct {
	mu        sync.Mutex
	responses map[string]func(req FetchRequest) ([]byte, error)
	seen      map[string][]string
	requests  map[string][]FetchRequest
}

func (f *fakeAPIConnector) Fetch(_ context.Context, req FetchRequest) (FetchResult, error) {
	if f.seen == nil {
		f.seen = map[string][]string{}
	}
	if f.requests == nil {
		f.requests = map[string][]FetchRequest{}
	}
	key := req.Path
	f.mu.Lock()
	f.requests[key] = append(f.requests[key], req)
	f.mu.Unlock()
	var marker string
	if len(req.QueryParams) > 0 {
		keys := make([]string, 0, len(req.QueryParams))
		for k := range req.QueryParams {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if req.QueryParams[k] == "" {
				continue
			}
			marker = req.QueryParams[k]
			break
		}
	}
	if marker != "" {
		f.mu.Lock()
		f.seen[key] = append(f.seen[key], marker)
		f.mu.Unlock()
	}
	handler := f.responses[key]
	if handler == nil {
		return FetchResult{}, fmt.Errorf("unexpected path %s", req.Path)
	}
	body, err := handler(req)
	if err != nil {
		return FetchResult{}, err
	}
	return FetchResult{Payload: body, StatusCode: 200, FinalURL: req.Path}, nil
}

func TestGeneratePlanChainedFKFlow(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/op1": func(req FetchRequest) ([]byte, error) { return []byte(`[{"id1":10},{"id1":20}]`), nil },
			"/op2": func(req FetchRequest) ([]byte, error) {
				switch req.QueryParams["id1"] {
				case "10":
					return []byte(`[{"id2":100}]`), nil
				case "20":
					return []byte(`[{"id2":200}]`), nil
				default:
					return nil, fmt.Errorf("unexpected id1")
				}
			},
			"/op3": func(req FetchRequest) ([]byte, error) {
				switch req.QueryParams["id2"] {
				case "100":
					return []byte(`[{"value":"v100"}]`), nil
				case "200":
					return []byte(`[{"value":"v200"}]`), nil
				default:
					return nil, fmt.Errorf("unexpected id2")
				}
			},
		},
	}

	service := NewService(api)
	plan, err := service.GeneratePlan(context.Background(), buildThreeStepSpec(), "https://example.com")
	if err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}
	if len(plan.Operations) == 0 {
		t.Fatalf("expected non-empty plan")
	}

	slices.Sort(api.seen["/op2"])
	slices.Sort(api.seen["/op3"])
	if !slices.Equal(api.seen["/op2"], []string{"10", "20"}) {
		t.Fatalf("unexpected op2 calls: %v", api.seen["/op2"])
	}
	if !slices.Equal(api.seen["/op3"], []string{"100", "200"}) {
		t.Fatalf("unexpected op3 calls: %v", api.seen["/op3"])
	}
}

func TestGeneratePlanSourceObjectFeedsChain(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/op1": func(req FetchRequest) ([]byte, error) { return []byte(`{"id1":77}`), nil },
			"/op2": func(req FetchRequest) ([]byte, error) { return []byte(`[{"id2":177}]`), nil },
			"/op3": func(req FetchRequest) ([]byte, error) { return []byte(`[{"value":"ok"}]`), nil },
		},
	}

	service := NewService(api)
	if _, err := service.GeneratePlan(context.Background(), buildThreeStepSpec(), "https://example.com"); err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}
	if !slices.Equal(api.seen["/op2"], []string{"77"}) {
		t.Fatalf("unexpected op2 calls: %v", api.seen["/op2"])
	}
}

func TestGeneratePlanWaitsForLaterDependencyProducer(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/datasets": func(req FetchRequest) ([]byte, error) {
				if req.QueryParams["publicationId"] == "" {
					return nil, fmt.Errorf("missing publicationId")
				}
				return []byte(`[{"id":101,"name":"dataset"}]`), nil
			},
			"/publications": func(req FetchRequest) ([]byte, error) {
				return []byte(`[{"id":77}]`), nil
			},
		},
	}

	publicationIDField := openapi3.NewIntegerSchema()
	publicationIDField.Extensions = map[string]any{"x-response-data": map[string]any{"id": "publicationId"}}
	publicationsSchema := openapi3.NewArraySchema().WithItems(
		openapi3.NewObjectSchema().WithProperty("id", publicationIDField),
	)

	datasetsSchema := openapi3.NewArraySchema().WithItems(
		openapi3.NewObjectSchema().
			WithProperty("id", withPK(openapi3.NewIntegerSchema())).
			WithProperty("name", openapi3.NewStringSchema()),
	)

	publicationOp := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Responses:  responseWithJSONSchema("#/components/schemas/publications", publicationsSchema),
	}

	paramPublicationID := openapi3.NewQueryParameter("publicationId").
		WithRequired(true).
		WithSchema(openapi3.NewIntegerSchema())
	paramPublicationID.Extensions = map[string]any{
		"x-param-data": map[string]any{"type": "operation", "operation-id": "publicationId"},
	}
	datasetsOp := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: paramPublicationID}},
		Responses:  responseWithJSONSchema("#/components/schemas/datasets", datasetsSchema),
	}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/datasets", &openapi3.PathItem{Get: datasetsOp}),
			openapi3.WithPath("/publications", &openapi3.PathItem{Get: publicationOp}),
		),
	}

	service := NewService(api)
	plan, err := service.GeneratePlan(context.Background(), spec, "https://example.com")
	if err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}

	if !slices.Equal(api.seen["/datasets"], []string{"77"}) {
		t.Fatalf("expected /datasets to be called after dependency discovery, got %v", api.seen["/datasets"])
	}

	foundInsert := false
	for _, op := range plan.Operations {
		insert, ok := op.(InsertRowsOp)
		if !ok || insert.TableName != "datasets" {
			continue
		}
		foundInsert = true
		break
	}
	if !foundInsert {
		t.Fatalf("expected datasets insert operation in plan")
	}
}

func TestGetAuthParamSpecs(t *testing.T) {
	service := NewService(nil)
	headerParam := openapi3.NewHeaderParameter("X-Token").WithRequired(true).WithSchema(openapi3.NewStringSchema())
	headerParam.Extensions = map[string]any{"x-auth": "API_TOKEN"}
	queryParam := openapi3.NewQueryParameter("api_key").WithSchema(openapi3.NewStringSchema())
	queryParam.Extensions = map[string]any{"x-auth": "API_KEY"}
	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{
			&openapi3.ParameterRef{Value: headerParam},
			&openapi3.ParameterRef{Value: queryParam},
		},
	}

	specs, err := service.getAuthParamSpecs("/secure", op)
	if err != nil {
		t.Fatalf("getAuthParamSpecs returned error: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 auth specs, got %d", len(specs))
	}
	if specs[0].ParamName != "X-Token" || specs[0].In != "header" || specs[0].EnvVar != "API_TOKEN" {
		t.Fatalf("unexpected header auth spec: %+v", specs[0])
	}
	if specs[1].ParamName != "api_key" || specs[1].In != "query" || specs[1].EnvVar != "API_KEY" {
		t.Fatalf("unexpected query auth spec: %+v", specs[1])
	}
}

func TestGetAuthParamSpecsRejectsUnsupportedLocation(t *testing.T) {
	service := NewService(nil)
	param := openapi3.NewPathParameter("id").WithRequired(true).WithSchema(openapi3.NewStringSchema())
	param.Extensions = map[string]any{"x-auth": "API_TOKEN"}
	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: param}},
	}

	_, err := service.getAuthParamSpecs("/secure/{id}", op)
	if err == nil || !strings.Contains(err.Error(), "supported only for header and query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGeneratePlanAppliesAuthFromEnv(t *testing.T) {
	t.Setenv("API_TOKEN", "super-secret")

	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/secure": func(req FetchRequest) ([]byte, error) {
				if req.Headers["X-Token"] != "super-secret" {
					return nil, fmt.Errorf("missing auth header")
				}
				return []byte(`[{"id":1,"name":"ok"}]`), nil
			},
		},
	}

	tokenParam := openapi3.NewHeaderParameter("X-Token").WithRequired(true).WithSchema(openapi3.NewStringSchema())
	tokenParam.Extensions = map[string]any{"x-auth": "API_TOKEN"}
	op := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: tokenParam}},
		Responses: responseWithJSONSchema("#/components/schemas/secure", openapi3.NewArraySchema().WithItems(
			openapi3.NewObjectSchema().
				WithProperty("id", withPK(openapi3.NewIntegerSchema())).
				WithProperty("name", openapi3.NewStringSchema()),
		)),
	}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths:   openapi3.NewPaths(openapi3.WithPath("/secure", &openapi3.PathItem{Get: op})),
	}

	service := NewService(api)
	plan, err := service.GeneratePlan(context.Background(), spec, "https://example.com")
	if err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}
	if len(api.requests["/secure"]) != 1 {
		t.Fatalf("expected one secure request, got %d", len(api.requests["/secure"]))
	}
	if api.requests["/secure"][0].Headers["X-Token"] != "super-secret" {
		t.Fatalf("unexpected auth header: %+v", api.requests["/secure"][0].Headers)
	}
	if !api.requests["/secure"][0].SensitiveHeaders["X-Token"] {
		t.Fatalf("expected auth header to be marked sensitive")
	}

	foundInsert := false
	for _, operation := range plan.Operations {
		insert, ok := operation.(InsertRowsOp)
		if ok && insert.TableName == "secure" {
			foundInsert = true
		}
	}
	if !foundInsert {
		t.Fatalf("expected secure insert operation")
	}
}

func TestGeneratePlanSkipsWhenAuthEnvMissing(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/secure": func(req FetchRequest) ([]byte, error) {
				return []byte(`[{"id":1}]`), nil
			},
		},
	}

	tokenParam := openapi3.NewHeaderParameter("X-Token").WithRequired(true).WithSchema(openapi3.NewStringSchema())
	tokenParam.Extensions = map[string]any{"x-auth": "MISSING_API_TOKEN"}
	op := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: tokenParam}},
		Responses: responseWithJSONSchema("#/components/schemas/secure", openapi3.NewArraySchema().WithItems(
			openapi3.NewObjectSchema().WithProperty("id", withPK(openapi3.NewIntegerSchema())),
		)),
	}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths:   openapi3.NewPaths(openapi3.WithPath("/secure", &openapi3.PathItem{Get: op})),
	}

	service := NewService(api)
	plan, err := service.GeneratePlan(context.Background(), spec, "https://example.com")
	if err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}
	if len(api.requests["/secure"]) != 0 {
		t.Fatalf("expected secure operation to be skipped, got %d requests", len(api.requests["/secure"]))
	}
	for _, operation := range plan.Operations {
		insert, ok := operation.(InsertRowsOp)
		if ok && insert.TableName == "secure" {
			t.Fatalf("did not expect secure insert operation when auth env is missing")
		}
	}
}

func TestGeneratePlanAuthOverridesFKValue(t *testing.T) {
	t.Setenv("PUBLICATION_TOKEN", "from-env")

	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/upstream": func(req FetchRequest) ([]byte, error) {
				return []byte(`[{"token":"from-upstream"}]`), nil
			},
			"/secure": func(req FetchRequest) ([]byte, error) {
				if req.QueryParams["token"] != "from-env" {
					return nil, fmt.Errorf("expected env token, got %s", req.QueryParams["token"])
				}
				return []byte(`[{"id":1}]`), nil
			},
		},
	}

	tokenField := openapi3.NewStringSchema()
	tokenField.Extensions = map[string]any{"x-response-data": map[string]any{"id": "token"}}
	upstreamOp := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Responses:  responseWithJSONSchema("#/components/schemas/upstream", openapi3.NewArraySchema().WithItems(openapi3.NewObjectSchema().WithProperty("token", tokenField))),
	}

	tokenParam := openapi3.NewQueryParameter("token").WithRequired(true).WithSchema(openapi3.NewStringSchema())
	tokenParam.Extensions = map[string]any{
		"x-param-data": map[string]any{"type": "operation", "operation-id": "token"},
		"x-auth":       "PUBLICATION_TOKEN",
	}
	secureOp := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: tokenParam}},
		Responses: responseWithJSONSchema("#/components/schemas/secure", openapi3.NewArraySchema().WithItems(
			openapi3.NewObjectSchema().WithProperty("id", withPK(openapi3.NewIntegerSchema())),
		)),
	}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/secure", &openapi3.PathItem{Get: secureOp}),
			openapi3.WithPath("/upstream", &openapi3.PathItem{Get: upstreamOp}),
		),
	}

	service := NewService(api)
	if _, err := service.GeneratePlan(context.Background(), spec, "https://example.com"); err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}
	if len(api.requests["/secure"]) != 1 {
		t.Fatalf("expected one secure request, got %d", len(api.requests["/secure"]))
	}
	if api.requests["/secure"][0].QueryParams["token"] != "from-env" {
		t.Fatalf("expected x-auth to override x-param-data, got %q", api.requests["/secure"][0].QueryParams["token"])
	}
	if !api.requests["/secure"][0].SensitiveQuery["token"] {
		t.Fatalf("expected auth query param to be marked sensitive")
	}
}

func TestBuildOperationExtractionPlanRelationShapes(t *testing.T) {
	service := NewService(nil)
	op := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Responses:  responseWithJSONSchema("#/components/schemas/root", markedRootSchema()),
	}

	plan, err := service.buildOperationExtractionPlan(op)
	if err != nil {
		t.Fatalf("buildOperationExtractionPlan returned error: %v", err)
	}
	if !plan.HasMarks {
		t.Fatalf("expected marked plan")
	}

	relations := map[string]relationKind{}
	joinTables := map[string]bool{}
	for _, rel := range plan.Relations {
		relations[rel.ParentTable+"->"+rel.ChildTable] = rel.Kind
		if rel.Kind == relationLinkTable {
			joinTables[rel.JoinTableName] = true
		}
	}

	if relations["users->settings"] != relationDirectRef {
		t.Fatalf("expected users->settings direct_ref")
	}
	if relations["users->phones"] != relationDirectRef {
		t.Fatalf("expected users->phones direct_ref")
	}
	if relations["orders->customers"] != relationDirectRef {
		t.Fatalf("expected orders->customers direct_ref")
	}
	if relations["orders->tags"] != relationLinkTable {
		t.Fatalf("expected orders->tags link_table")
	}
	if !joinTables["orders_tags_link"] {
		t.Fatalf("expected orders_tags_link join table")
	}
}

func TestBuildOperationExtractionPlanRequiresSinglePK(t *testing.T) {
	service := NewService(nil)

	bad := openapi3.NewObjectSchema().WithProperty("id", openapi3.NewIntegerSchema())
	bad.Extensions = map[string]any{"x-table-name": "bad_table"}

	op := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Responses:  responseWithJSONSchema("#/components/schemas/bad", bad),
	}
	_, err := service.buildOperationExtractionPlan(op)
	if err == nil || !strings.Contains(err.Error(), "exactly one x-pk") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGeneratePlanMarkedModeRecursiveInserts(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/marked": func(req FetchRequest) ([]byte, error) {
				return []byte(`{
					"usersPayload": {
						"id": 1,
						"name": "u1",
						"settings": {"sid": 9, "name": "cfg"},
						"phones": [{"pid": 10, "value": "111"}, {"pid": 11, "value": "222"}]
					},
					"ordersPayload": [
						{
							"oid": 100,
							"customer": {"cid": 7, "title": "c1"},
							"tags": [{"tid": 55, "label": "new"}, {"tid": 56, "label": "vip"}]
						}
					]
				}`), nil
			},
		},
	}

	service := NewService(api)
	plan, err := service.GeneratePlan(context.Background(), markedSpec(), "https://example.com")
	if err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}

	var createTables []string
	var insertOps int
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case CreateTableOp:
			createTables = append(createTables, typed.TableName)
		case CreateLinkTableOp:
			createTables = append(createTables, typed.TableName)
		case InsertRowsOp:
			insertOps += len(typed.Rows)
		}
	}

	for _, expected := range []string{"users", "settings", "phones", "orders", "customers", "tags", "orders_tags_link"} {
		if !slices.Contains(createTables, expected) {
			t.Fatalf("missing create table op for %s", expected)
		}
	}
	if insertOps == 0 {
		t.Fatalf("expected insert operations")
	}
}

func TestGeneratePlanCursorPagination(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/paged": func(req FetchRequest) ([]byte, error) {
				switch req.QueryParams["cursor"] {
				case "":
					return []byte(`{"id":1,"next":{"cursor":"abc"}}`), nil
				case "abc":
					return []byte(`{"id":2,"next":{"cursor":""}}`), nil
				default:
					return nil, fmt.Errorf("unexpected cursor %q", req.QueryParams["cursor"])
				}
			},
		},
	}

	itemsSchema := openapi3.NewObjectSchema().
		WithProperty("id", withPK(openapi3.NewIntegerSchema())).
		WithProperty("next", openapi3.NewObjectSchema().WithProperty("cursor", openapi3.NewStringSchema()))

	cursorParam := openapi3.NewQueryParameter("cursor").WithSchema(openapi3.NewStringSchema())
	cursorParam.Extensions = map[string]any{"x-param-data": map[string]any{"type": "cursor", "cursor": "next.cursor"}}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/paged", &openapi3.PathItem{Get: &openapi3.Operation{
				Extensions: map[string]any{"x-res-type": "one-shot"},
				Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: cursorParam}},
				Responses:  responseWithJSONSchema("#/components/schemas/items", itemsSchema),
			}}),
		),
	}

	service := NewService(api)
	plan, err := service.GeneratePlan(context.Background(), spec, "https://example.com")
	if err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}

	if !slices.Equal(api.seen["/paged"], []string{"abc"}) {
		t.Fatalf("expected second paginated request marker, got %v", api.seen["/paged"])
	}

	var rowCount int
	for _, op := range plan.Operations {
		insert, ok := op.(InsertRowsOp)
		if ok && insert.TableName == "items" {
			rowCount += len(insert.Rows)
		}
	}
	if rowCount != 2 {
		t.Fatalf("expected 2 inserted rows across pages, got %d", rowCount)
	}
}

func TestGeneratePlanOffsetPaginationPerDependencyCombination(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/sources": func(req FetchRequest) ([]byte, error) {
				return []byte(`[{"id":10},{"id":20}]`), nil
			},
			"/paged": func(req FetchRequest) ([]byte, error) {
				key := req.QueryParams["sourceId"] + ":" + req.QueryParams["offset"]
				switch key {
				case "10:0":
					return []byte(`[{"value":"a"}]`), nil
				case "10:1":
					return []byte(`[]`), nil
				case "20:0":
					return []byte(`[{"value":"b"}]`), nil
				case "20:1":
					return []byte(`[]`), nil
				default:
					return nil, fmt.Errorf("unexpected request %s", key)
				}
			},
		},
	}

	sourceIDField := openapi3.NewIntegerSchema()
	sourceIDField.Extensions = map[string]any{"x-response-data": map[string]any{"id": "sourceId"}}
	sourceSchema := openapi3.NewArraySchema().WithItems(openapi3.NewObjectSchema().WithProperty("id", sourceIDField))

	sourceParam := openapi3.NewQueryParameter("sourceId").WithRequired(true).WithSchema(openapi3.NewIntegerSchema())
	sourceParam.Extensions = map[string]any{"x-param-data": map[string]any{"type": "operation", "operation-id": "sourceId"}}

	offsetParam := openapi3.NewQueryParameter("offset").WithSchema(openapi3.NewIntegerSchema())
	offsetParam.Extensions = map[string]any{"x-param-data": map[string]any{"type": "offset", "offset": map[string]any{"start": 0, "increment": 1}}}

	pagedSchema := openapi3.NewArraySchema().WithItems(openapi3.NewObjectSchema().WithProperty("value", openapi3.NewStringSchema()))

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/paged", &openapi3.PathItem{Get: &openapi3.Operation{
				Extensions: map[string]any{"x-res-type": "one-shot"},
				Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: sourceParam}, &openapi3.ParameterRef{Value: offsetParam}},
				Responses:  responseWithJSONSchema("#/components/schemas/paged", pagedSchema),
			}}),
			openapi3.WithPath("/sources", &openapi3.PathItem{Get: &openapi3.Operation{
				Extensions: map[string]any{"x-res-type": "one-shot"},
				Responses:  responseWithJSONSchema("#/components/schemas/sources", sourceSchema),
			}}),
		),
	}

	service := NewService(api)
	plan, err := service.GeneratePlan(context.Background(), spec, "https://example.com")
	if err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}

	if got := api.seen["/paged"]; len(got) != 4 {
		t.Fatalf("expected 4 paginated calls, got %v", got)
	}

	var rowCount int
	for _, op := range plan.Operations {
		insert, ok := op.(InsertRowsOp)
		if ok && insert.TableName == "paged" {
			rowCount += len(insert.Rows)
		}
	}
	if rowCount != 2 {
		t.Fatalf("expected 2 inserted rows across combinations, got %d", rowCount)
	}
}

func buildThreeStepSpec() *openapi3.T {
	id1Field := openapi3.NewIntegerSchema()
	id1Field.Extensions = map[string]any{"x-response-data": map[string]any{"id": "id1"}}

	id2Field := openapi3.NewIntegerSchema()
	id2Field.Extensions = map[string]any{"x-response-data": map[string]any{"id": "id2"}}

	op1Schema := openapi3.NewArraySchema().WithItems(openapi3.NewObjectSchema().WithProperty("id1", id1Field))
	op2Schema := openapi3.NewArraySchema().WithItems(openapi3.NewObjectSchema().WithProperty("id2", id2Field))
	op3Schema := openapi3.NewArraySchema().WithItems(openapi3.NewObjectSchema().WithProperty("value", openapi3.NewStringSchema()))

	op1 := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Responses:  responseWithJSONSchema("#/components/schemas/op1", op1Schema),
	}

	paramID1 := openapi3.NewQueryParameter("id1").WithRequired(true).WithSchema(openapi3.NewIntegerSchema())
	paramID1.Extensions = map[string]any{"x-param-data": map[string]any{"type": "operation", "operation-id": "id1"}}
	op2 := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: paramID1}},
		Responses:  responseWithJSONSchema("#/components/schemas/op2", op2Schema),
	}

	paramID2 := openapi3.NewQueryParameter("id2").WithRequired(true).WithSchema(openapi3.NewIntegerSchema())
	paramID2.Extensions = map[string]any{"x-param-data": map[string]any{"type": "operation", "operation-id": "id2"}}
	op3 := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: paramID2}},
		Responses:  responseWithJSONSchema("#/components/schemas/op3", op3Schema),
	}

	return &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/op1", &openapi3.PathItem{Get: op1}),
			openapi3.WithPath("/op2", &openapi3.PathItem{Get: op2}),
			openapi3.WithPath("/op3", &openapi3.PathItem{Get: op3}),
		),
	}
}

func markedRootSchema() *openapi3.Schema {
	usersSchema := openapi3.NewObjectSchema().
		WithProperty("id", withPK(openapi3.NewIntegerSchema())).
		WithProperty("name", openapi3.NewStringSchema()).
		WithProperty("settings", openapi3.NewObjectSchema().
			WithProperty("sid", withPK(openapi3.NewIntegerSchema())).
			WithProperty("name", openapi3.NewStringSchema())).
		WithProperty("phones", openapi3.NewArraySchema().WithItems(
			openapi3.NewObjectSchema().
				WithProperty("pid", withPK(openapi3.NewIntegerSchema())).
				WithProperty("value", openapi3.NewStringSchema()),
		))
	usersSchema.Extensions = map[string]any{"x-table-name": "users"}
	usersSchema.Properties["settings"].Value.Extensions = map[string]any{"x-table-name": "settings"}
	usersSchema.Properties["phones"].Value.Items.Value.Extensions = map[string]any{"x-table-name": "phones"}

	ordersSchema := openapi3.NewArraySchema().WithItems(
		openapi3.NewObjectSchema().
			WithProperty("oid", withPK(openapi3.NewIntegerSchema())).
			WithProperty("customer", openapi3.NewObjectSchema().
				WithProperty("cid", withPK(openapi3.NewIntegerSchema())).
				WithProperty("title", openapi3.NewStringSchema())).
			WithProperty("tags", openapi3.NewArraySchema().WithItems(
				openapi3.NewObjectSchema().
					WithProperty("tid", withPK(openapi3.NewIntegerSchema())).
					WithProperty("label", openapi3.NewStringSchema()),
			)),
	)
	ordersSchema.Items.Value.Extensions = map[string]any{"x-table-name": "orders"}
	ordersSchema.Items.Value.Properties["customer"].Value.Extensions = map[string]any{"x-table-name": "customers"}
	ordersSchema.Items.Value.Properties["tags"].Value.Items.Value.Extensions = map[string]any{"x-table-name": "tags"}

	return openapi3.NewObjectSchema().
		WithProperty("usersPayload", usersSchema).
		WithProperty("ordersPayload", ordersSchema)
}

func markedSpec() *openapi3.T {
	return &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/marked", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: map[string]any{"x-res-type": "one-shot"},
					Responses:  responseWithJSONSchema("#/components/schemas/root", markedRootSchema()),
				},
			}),
		),
	}
}

func responseWithJSONSchema(ref string, schema *openapi3.Schema) *openapi3.Responses {
	return openapi3.NewResponses(
		openapi3.WithStatus(200, &openapi3.ResponseRef{
			Value: openapi3.NewResponse().
				WithDescription("ok").
				WithContent(openapi3.Content{
					"application/json": &openapi3.MediaType{
						Schema: openapi3.NewSchemaRef(ref, schema),
					},
				}),
		}),
	)
}

func withPK(schema *openapi3.Schema) *openapi3.Schema {
	if schema.Extensions == nil {
		schema.Extensions = map[string]any{}
	}
	schema.Extensions["x-pk"] = true
	return schema
}
