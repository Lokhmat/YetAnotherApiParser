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

func TestGenerateCyclePlanOneShotSkipsAfterCommit(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/items": func(req FetchRequest) ([]byte, error) {
				return []byte(`[{"id":1,"name":"first"}]`), nil
			},
		},
	}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/items", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: map[string]any{"x-res-type": "one-shot"},
					Responses: responseWithJSONSchema("#/components/schemas/items", openapi3.NewArraySchema().WithItems(
						openapi3.NewObjectSchema().
							WithProperty("id", withPK(openapi3.NewIntegerSchema())).
							WithProperty("name", openapi3.NewStringSchema()),
					)),
				},
			}),
		),
	}

	service := NewService(api)
	store := memoryCheckpointStore{}

	firstPlan, err := service.GenerateCyclePlan(context.Background(), spec, "https://example.com", store)
	if err != nil {
		t.Fatalf("GenerateCyclePlan returned error: %v", err)
	}
	if len(api.requests["/items"]) != 1 {
		t.Fatalf("expected one fetch during first cycle, got %d", len(api.requests["/items"]))
	}

	service.CommitCycle(firstPlan)

	secondPlan, err := service.GenerateCyclePlan(context.Background(), spec, "https://example.com", store)
	if err != nil {
		t.Fatalf("GenerateCyclePlan second cycle returned error: %v", err)
	}
	if len(api.requests["/items"]) != 1 {
		t.Fatalf("expected one-shot operation to be skipped after commit, got %d requests", len(api.requests["/items"]))
	}
	if secondPlan.UpsertPlan == nil || len(secondPlan.UpsertPlan.Operations) != 0 {
		t.Fatalf("expected empty upsert plan after one-shot commit, got %+v", secondPlan.UpsertPlan)
	}
}

func TestGenerateCyclePlanIncrementalUsesCheckpoint(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/events": func(req FetchRequest) ([]byte, error) {
				if req.QueryParams["cursor"] != "saved-token" {
					return nil, fmt.Errorf("unexpected cursor %q", req.QueryParams["cursor"])
				}
				return []byte(`{"id":2,"next":{"cursor":""}}`), nil
			},
		},
	}

	cursorParam := openapi3.NewQueryParameter("cursor").WithSchema(openapi3.NewStringSchema())
	cursorParam.Extensions = map[string]any{"x-param-data": map[string]any{"type": "cursor", "cursor": "next.cursor"}}
	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/events", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: map[string]any{"x-res-type": "incremental"},
					Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: cursorParam}},
					Responses: responseWithJSONSchema("#/components/schemas/events", openapi3.NewObjectSchema().
						WithProperty("id", withPK(openapi3.NewIntegerSchema())).
						WithProperty("next", openapi3.NewObjectSchema().WithProperty("cursor", openapi3.NewStringSchema())),
					),
				},
			}),
		),
	}

	service := NewService(api)
	store := memoryCheckpointStore{}
	key, err := buildCheckpointKey("GET", "/events", map[string]string{}, "cursor")
	if err != nil {
		t.Fatalf("buildCheckpointKey returned error: %v", err)
	}
	checkpoint, err := buildCheckpoint("GET", "/events", map[string]string{}, &ParamDataSpec{ParamName: "cursor", Type: paramDataTypeCursor}, key, "saved-token")
	if err != nil {
		t.Fatalf("buildCheckpoint returned error: %v", err)
	}
	store[checkpoint.Key] = checkpoint

	plan, err := service.GenerateCyclePlan(context.Background(), spec, "https://example.com", store)
	if err != nil {
		t.Fatalf("GenerateCyclePlan returned error: %v", err)
	}
	if len(api.requests["/events"]) != 1 {
		t.Fatalf("expected one incremental request, got %d", len(api.requests["/events"]))
	}
	if api.requests["/events"][0].QueryParams["cursor"] != "saved-token" {
		t.Fatalf("expected checkpoint cursor to be used, got %+v", api.requests["/events"][0].QueryParams)
	}
	if len(plan.PendingCheckpoints) != 1 || plan.PendingCheckpoints[0].Key != key {
		t.Fatalf("expected one staged checkpoint for %s, got %+v", key, plan.PendingCheckpoints)
	}
}

func TestGenerateCyclePlanHeadWatermarkStagesCheckpointFromFullFetch(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/events": func(req FetchRequest) ([]byte, error) {
				switch req.QueryParams["cursor"] {
				case "":
					return []byte(`{
						"data":{"items":[
							{"id":3,"updated_at":"2026-04-03T00:00:00Z","value":"three"},
							{"id":2,"updated_at":"2026-04-03T00:00:00Z","value":"two"}
						]},
						"page":{"next_cursor":"c1"}
					}`), nil
				case "c1":
					return []byte(`{
						"data":{"items":[
							{"id":1,"updated_at":"2026-04-02T00:00:00Z","value":"one"}
						]},
						"page":{"next_cursor":""}
					}`), nil
				default:
					return nil, fmt.Errorf("unexpected cursor %q", req.QueryParams["cursor"])
				}
			},
		},
	}

	service := NewService(api)
	plan, err := service.GenerateCyclePlan(context.Background(), headWatermarkMarkedSpec(false, false), "https://example.com", memoryCheckpointStore{})
	if err != nil {
		t.Fatalf("GenerateCyclePlan returned error: %v", err)
	}
	if len(api.requests["/events"]) != 2 {
		t.Fatalf("expected 2 paginated requests, got %d", len(api.requests["/events"]))
	}
	if got := insertedPrimaryKeysForTable(plan.UpsertPlan, "events", "id"); !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Fatalf("unexpected inserted ids: %v", got)
	}
	if len(plan.PendingCheckpoints) != 1 {
		t.Fatalf("expected one pending checkpoint, got %+v", plan.PendingCheckpoints)
	}
	checkpointState, compatible, err := parseHeadWatermarkCheckpoint(plan.PendingCheckpoints[0].ResumeValueJSON)
	if err != nil {
		t.Fatalf("parseHeadWatermarkCheckpoint returned error: %v", err)
	}
	if !compatible || checkpointState == nil {
		t.Fatalf("expected head-watermark checkpoint, got compatible=%v state=%+v", compatible, checkpointState)
	}
	if watermark, ok := checkpointState.WatermarkValue.(string); !ok || watermark != "2026-04-03T00:00:00Z" {
		t.Fatalf("unexpected watermark value: %+v", checkpointState)
	}
	if got := sortedBoundaryKeys(checkpointState.BoundaryKeys); !slices.Equal(got, []string{"[2]", "[3]"}) {
		t.Fatalf("unexpected boundary keys: %v", got)
	}
}

func TestGenerateCyclePlanHeadWatermarkUsesBoundaryKeysAndRestartsFromHead(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/events": func(req FetchRequest) ([]byte, error) {
				switch req.QueryParams["cursor"] {
				case "":
					return []byte(`{
						"data":{"items":[
							{"id":4,"updated_at":"2026-04-03T00:00:00Z","value":"new-equal"},
							{"id":2,"updated_at":"2026-04-03T00:00:00Z","value":"old-equal"}
						]},
						"page":{"next_cursor":"c1"}
					}`), nil
				case "c1":
					return []byte(`{
						"data":{"items":[
							{"id":1,"updated_at":"2026-04-02T00:00:00Z","value":"old"}
						]},
						"page":{"next_cursor":""}
					}`), nil
				default:
					return nil, fmt.Errorf("unexpected cursor %q", req.QueryParams["cursor"])
				}
			},
		},
	}

	service := NewService(api)
	store := memoryCheckpointStore{}
	key, err := buildCheckpointKey("GET", "/events", map[string]string{}, "cursor")
	if err != nil {
		t.Fatalf("buildCheckpointKey returned error: %v", err)
	}
	checkpoint, err := buildHeadWatermarkCheckpoint("GET", "/events", map[string]string{}, &ParamDataSpec{ParamName: "cursor", Type: paramDataTypeCursor}, key, &headWatermarkCycleState{
		Observed:      true,
		WatermarkType: watermarkTypeDateTime,
		MaxWatermark:  "2026-04-03T00:00:00Z",
		BoundaryKeys:  map[string]bool{"[2]": true},
	})
	if err != nil {
		t.Fatalf("buildHeadWatermarkCheckpoint returned error: %v", err)
	}
	store[key] = checkpoint

	plan, err := service.GenerateCyclePlan(context.Background(), headWatermarkMarkedSpec(false, false), "https://example.com", store)
	if err != nil {
		t.Fatalf("GenerateCyclePlan returned error: %v", err)
	}
	if len(api.requests["/events"]) != 2 {
		t.Fatalf("expected 2 paginated requests, got %d", len(api.requests["/events"]))
	}
	if _, ok := api.requests["/events"][0].QueryParams["cursor"]; ok {
		t.Fatalf("expected first request to restart from head, got %+v", api.requests["/events"][0].QueryParams)
	}
	if got := insertedPrimaryKeysForTable(plan.UpsertPlan, "events", "id"); !slices.Equal(got, []string{"4"}) {
		t.Fatalf("expected only unseen equal-watermark row to be inserted, got %v", got)
	}
	if len(plan.PendingCheckpoints) != 1 {
		t.Fatalf("expected one pending checkpoint, got %+v", plan.PendingCheckpoints)
	}
	checkpointState, compatible, err := parseHeadWatermarkCheckpoint(plan.PendingCheckpoints[0].ResumeValueJSON)
	if err != nil {
		t.Fatalf("parseHeadWatermarkCheckpoint returned error: %v", err)
	}
	if !compatible || checkpointState == nil {
		t.Fatalf("expected head-watermark checkpoint, got compatible=%v state=%+v", compatible, checkpointState)
	}
	if got := sortedBoundaryKeys(checkpointState.BoundaryKeys); !slices.Equal(got, []string{"[2]", "[4]"}) {
		t.Fatalf("unexpected boundary keys: %v", got)
	}
}

func TestGenerateCyclePlanHeadWatermarkPublishesOnlyFilteredDependencyValues(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/details": func(req FetchRequest) ([]byte, error) {
				switch req.QueryParams["eventId"] {
				case "4":
					return []byte(`[{"id":400}]`), nil
				default:
					return nil, fmt.Errorf("unexpected eventId %q", req.QueryParams["eventId"])
				}
			},
			"/events": func(req FetchRequest) ([]byte, error) {
				switch req.QueryParams["cursor"] {
				case "":
					return []byte(`{
						"data":{"items":[
							{"id":4,"updated_at":"2026-04-03T00:00:00Z","value":"new-equal"},
							{"id":2,"updated_at":"2026-04-03T00:00:00Z","value":"old-equal"}
						]},
						"page":{"next_cursor":"c1"}
					}`), nil
				case "c1":
					return []byte(`{
						"data":{"items":[
							{"id":1,"updated_at":"2026-04-02T00:00:00Z","value":"old"}
						]},
						"page":{"next_cursor":""}
					}`), nil
				default:
					return nil, fmt.Errorf("unexpected cursor %q", req.QueryParams["cursor"])
				}
			},
		},
	}

	service := NewService(api)
	store := memoryCheckpointStore{}
	key, err := buildCheckpointKey("GET", "/events", map[string]string{}, "cursor")
	if err != nil {
		t.Fatalf("buildCheckpointKey returned error: %v", err)
	}
	checkpoint, err := buildHeadWatermarkCheckpoint("GET", "/events", map[string]string{}, &ParamDataSpec{ParamName: "cursor", Type: paramDataTypeCursor}, key, &headWatermarkCycleState{
		Observed:      true,
		WatermarkType: watermarkTypeDateTime,
		MaxWatermark:  "2026-04-03T00:00:00Z",
		BoundaryKeys:  map[string]bool{"[2]": true},
	})
	if err != nil {
		t.Fatalf("buildHeadWatermarkCheckpoint returned error: %v", err)
	}
	store[key] = checkpoint

	_, err = service.GenerateCyclePlan(context.Background(), headWatermarkMarkedSpec(true, false), "https://example.com", store)
	if err != nil {
		t.Fatalf("GenerateCyclePlan returned error: %v", err)
	}
	if len(api.requests["/details"]) != 1 {
		t.Fatalf("expected one downstream request, got %d", len(api.requests["/details"]))
	}
	if api.requests["/details"][0].QueryParams["eventId"] != "4" {
		t.Fatalf("expected only filtered id to be published, got %+v", api.requests["/details"][0].QueryParams)
	}
}

func TestGenerateCyclePlanRejectsHeadWatermarkOnNonIncrementalOperation(t *testing.T) {
	service := NewService(&fakeAPIConnector{})
	_, err := service.GenerateCyclePlan(context.Background(), headWatermarkMarkedSpec(false, true), "https://example.com", memoryCheckpointStore{})
	if err == nil || !strings.Contains(err.Error(), "x-incremental is allowed only when x-res-type=incremental") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateCyclePlanRejectsHeadWatermarkWithInvalidItemsPath(t *testing.T) {
	service := NewService(&fakeAPIConnector{})
	_, err := service.GenerateCyclePlan(context.Background(), headWatermarkMarkedSpec(false, false, map[string]any{
		"strategy":   "head-watermark",
		"items-path": "data.missing",
		"watermark": map[string]any{
			"path": "updated_at",
			"type": "datetime",
		},
		"key-paths": []any{"id"},
	}), "https://example.com", memoryCheckpointStore{})
	if err == nil || !strings.Contains(err.Error(), "invalid x-incremental.items-path") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateCyclePlanHeadWatermarkIgnoresLegacyCheckpointShape(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/events": func(req FetchRequest) ([]byte, error) {
				switch req.QueryParams["cursor"] {
				case "":
					return []byte(`{
						"data":{"items":[
							{"id":3,"updated_at":"2026-04-03T00:00:00Z","value":"three"}
						]},
						"page":{"next_cursor":"c1"}
					}`), nil
				case "c1":
					return []byte(`{
						"data":{"items":[]},
						"page":{"next_cursor":""}
					}`), nil
				default:
					return nil, fmt.Errorf("unexpected cursor %q", req.QueryParams["cursor"])
				}
			},
		},
	}

	service := NewService(api)
	store := memoryCheckpointStore{}
	key, err := buildCheckpointKey("GET", "/events", map[string]string{}, "cursor")
	if err != nil {
		t.Fatalf("buildCheckpointKey returned error: %v", err)
	}
	legacyCheckpoint, err := buildCheckpoint("GET", "/events", map[string]string{}, &ParamDataSpec{ParamName: "cursor", Type: paramDataTypeCursor}, key, "saved-token")
	if err != nil {
		t.Fatalf("buildCheckpoint returned error: %v", err)
	}
	store[key] = legacyCheckpoint

	_, err = service.GenerateCyclePlan(context.Background(), headWatermarkMarkedSpec(false, false), "https://example.com", store)
	if err != nil {
		t.Fatalf("GenerateCyclePlan returned error: %v", err)
	}
	if len(api.requests["/events"]) == 0 {
		t.Fatalf("expected requests to be made")
	}
	if _, ok := api.requests["/events"][0].QueryParams["cursor"]; ok {
		t.Fatalf("expected legacy checkpoint to be ignored for head-watermark mode, got %+v", api.requests["/events"][0].QueryParams)
	}
}

func TestGenerateCyclePlanHeadWatermarkSupportsRootArrayItemsPath(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/commits": func(req FetchRequest) ([]byte, error) {
				switch req.QueryParams["page"] {
				case "1":
					return []byte(`[
							{"sha":"a1","node_id":"n1","commit":{"url":"u1","author":{"date":"2026-04-03T00:00:00Z"}}},
							{"sha":"a2","node_id":"n2","commit":{"url":"u2","author":{"date":"2026-04-03T00:00:00Z"}}}
						]`), nil
				case "2":
					return []byte(`[
						{"sha":"a0","node_id":"n0","commit":{"url":"u0","author":{"date":"2026-04-02T00:00:00Z"}}}
					]`), nil
				case "3":
					return []byte(`[]`), nil
				default:
					return nil, fmt.Errorf("unexpected page %q", req.QueryParams["page"])
				}
			},
		},
	}

	service := NewService(api)
	plan, err := service.GenerateCyclePlan(context.Background(), headWatermarkRootArraySpec(), "https://example.com", memoryCheckpointStore{})
	if err != nil {
		t.Fatalf("GenerateCyclePlan returned error: %v", err)
	}
	if len(api.requests["/commits"]) != 3 {
		t.Fatalf("expected 3 paginated requests including final empty page, got %d", len(api.requests["/commits"]))
	}
	if got := insertedPrimaryKeysForTable(plan.UpsertPlan, "commit_outer", "sha"); !slices.Equal(got, []string{"a0", "a1", "a2"}) {
		t.Fatalf("unexpected inserted shas: %v", got)
	}
	if len(plan.PendingCheckpoints) != 1 {
		t.Fatalf("expected one pending checkpoint, got %+v", plan.PendingCheckpoints)
	}
	checkpointState, compatible, err := parseHeadWatermarkCheckpoint(plan.PendingCheckpoints[0].ResumeValueJSON)
	if err != nil {
		t.Fatalf("parseHeadWatermarkCheckpoint returned error: %v", err)
	}
	if !compatible || checkpointState == nil {
		t.Fatalf("expected head-watermark checkpoint, got compatible=%v state=%+v", compatible, checkpointState)
	}
	if got := sortedBoundaryKeys(checkpointState.BoundaryKeys); !slices.Equal(got, []string{"[\"n1\"]", "[\"n2\"]"}) {
		t.Fatalf("unexpected boundary keys: %v", got)
	}
}

func TestGenerateCyclePlanRejectsRootArrayWatermarkPathOnlyWhenWrongRelativePath(t *testing.T) {
	service := NewService(&fakeAPIConnector{})
	_, err := service.GenerateCyclePlan(context.Background(), headWatermarkRootArraySpec(map[string]any{
		"strategy":   "head-watermark",
		"items-path": "$",
		"watermark": map[string]any{
			"path": "author.date",
			"type": "datetime",
		},
		"key-paths": []any{"node_id"},
	}), "https://example.com", memoryCheckpointStore{})
	if err == nil || !strings.Contains(err.Error(), "invalid x-incremental.watermark.path") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateCyclePlanRejectsIncrementalWithoutPagination(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/events": func(req FetchRequest) ([]byte, error) { return []byte(`[{"id":1}]`), nil },
		},
	}
	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/events", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: map[string]any{"x-res-type": "incremental"},
					Responses: responseWithJSONSchema("#/components/schemas/events", openapi3.NewArraySchema().WithItems(
						openapi3.NewObjectSchema().WithProperty("id", withPK(openapi3.NewIntegerSchema())),
					)),
				},
			}),
		),
	}

	service := NewService(api)
	_, err := service.GenerateCyclePlan(context.Background(), spec, "https://example.com", memoryCheckpointStore{})
	if err == nil || !strings.Contains(err.Error(), "requires cursor or offset pagination") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateCyclePlanRejectsMixedTableModes(t *testing.T) {
	schema := openapi3.NewArraySchema().WithItems(
		openapi3.NewObjectSchema().WithProperty("id", withPK(openapi3.NewIntegerSchema())),
	)
	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/one-shot", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: map[string]any{"x-res-type": "one-shot"},
					Responses:  responseWithJSONSchema("#/components/schemas/shared", schema),
				},
			}),
			openapi3.WithPath("/full", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: map[string]any{"x-res-type": "full-reload"},
					Responses:  responseWithJSONSchema("#/components/schemas/shared", schema),
				},
			}),
		),
	}

	service := NewService(&fakeAPIConnector{})
	_, err := service.GenerateCyclePlan(context.Background(), spec, "https://example.com", memoryCheckpointStore{})
	if err == nil || !strings.Contains(err.Error(), "mixed x-res-type modes") {
		t.Fatalf("unexpected error: %v", err)
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

func TestGenerateFullSyncPlanBuildsDesiredRows(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/marked": func(req FetchRequest) ([]byte, error) {
				return []byte(`{
					"usersPayload": {
						"id": 1,
						"name": "alice",
						"settings": {"sid": 10, "name": "dark"},
						"phones": [{"pid": 100, "value": "123"}]
					},
					"ordersPayload": [{
						"oid": 7,
						"customer": {"cid": 8, "title": "bob"},
						"tags": [{"tid": 9, "label": "hot"}]
					}]
				}`), nil
			},
		},
	}

	service := NewService(api)
	plan, err := service.GenerateFullSyncPlan(context.Background(), markedSpec(), "https://example.com")
	if err != nil {
		t.Fatalf("GenerateFullSyncPlan returned error: %v", err)
	}

	users := fullSyncTableByName(t, plan, "users")
	if len(users.PrimaryKey) != 1 || users.PrimaryKey[0] != "id" {
		t.Fatalf("unexpected users primary key: %+v", users.PrimaryKey)
	}
	if len(users.Rows) != 1 {
		t.Fatalf("expected 1 user row, got %d", len(users.Rows))
	}
	if users.Rows[0].Columns[0] != "id" || users.Rows[0].Columns[1] != "name" {
		t.Fatalf("expected normalized column order, got %+v", users.Rows[0].Columns)
	}

	link := fullSyncTableByName(t, plan, "orders_tags_link")
	if len(link.Rows) != 1 {
		t.Fatalf("expected 1 link row, got %d", len(link.Rows))
	}
}

func TestGenerateFullSyncPlanFailsWithoutPrimaryKey(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/items": func(req FetchRequest) ([]byte, error) {
				return []byte(`[{"name":"x"}]`), nil
			},
		},
	}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/items", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: map[string]any{"x-res-type": "one-shot"},
					Responses: responseWithJSONSchema("#/components/schemas/items",
						openapi3.NewArraySchema().WithItems(
							openapi3.NewObjectSchema().WithProperty("name", openapi3.NewStringSchema()),
						),
					),
				},
			}),
		),
	}

	service := NewService(api)
	_, err := service.GenerateFullSyncPlan(context.Background(), spec, "https://example.com")
	if err == nil || !strings.Contains(err.Error(), "primary key metadata") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGeneratePlanRelaxesRequiredColumnsWhenRowsContainNullsOrMissingValues(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/commits": func(req FetchRequest) ([]byte, error) {
				return []byte(`[
					{"id":1,"author":null,"message":"first"},
					{"id":2}
				]`), nil
			},
		},
	}

	commitSchema := openapi3.NewArraySchema().WithItems(
		openapi3.NewObjectSchema().
			WithProperty("id", withPK(openapi3.NewIntegerSchema())).
			WithProperty("author", openapi3.NewStringSchema()).
			WithProperty("message", openapi3.NewStringSchema()).
			WithRequired([]string{"id", "author", "message"}),
	)

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/commits", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: map[string]any{"x-res-type": "one-shot"},
					Responses:  responseWithJSONSchema("#/components/schemas/commits", commitSchema),
				},
			}),
		),
	}

	service := NewService(api)
	plan, err := service.GeneratePlan(context.Background(), spec, "https://example.com")
	if err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}

	var create CreateTableOp
	found := false
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case CreateTableOp:
			if typed.TableName == "commits" {
				create = typed
				found = true
			}
		case *CreateTableOp:
			if typed.TableName == "commits" {
				create = *typed
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected create table op for commits")
	}

	nullableByName := make(map[string]bool)
	pkByName := make(map[string]bool)
	for _, col := range create.Columns {
		nullableByName[col.Name] = col.Nullable
		pkByName[col.Name] = col.PrimaryKey
	}
	if !pkByName["id"] {
		t.Fatalf("expected id to remain primary key, got %+v", create.Columns)
	}
	if !nullableByName["author"] {
		t.Fatalf("expected author to relax to nullable, got %+v", create.Columns)
	}
	if !nullableByName["message"] {
		t.Fatalf("expected message to relax to nullable because a row omitted it, got %+v", create.Columns)
	}
}

func headWatermarkMarkedSpec(includeDownstream bool, nonIncremental bool, overrides ...map[string]any) *openapi3.T {
	eventIDField := withPK(openapi3.NewIntegerSchema())
	if includeDownstream {
		eventIDField.Extensions["x-response-data"] = map[string]any{"id": "eventId"}
	}

	itemSchema := openapi3.NewObjectSchema().
		WithProperty("id", eventIDField).
		WithProperty("updated_at", openapi3.NewStringSchema()).
		WithProperty("value", openapi3.NewStringSchema())
	itemSchema.Extensions = map[string]any{"x-table-name": "events"}

	rootSchema := openapi3.NewObjectSchema().
		WithProperty("data", openapi3.NewObjectSchema().WithProperty("items", openapi3.NewArraySchema().WithItems(itemSchema))).
		WithProperty("page", openapi3.NewObjectSchema().WithProperty("next_cursor", openapi3.NewStringSchema()))

	cursorParam := openapi3.NewQueryParameter("cursor").WithSchema(openapi3.NewStringSchema())
	cursorParam.Extensions = map[string]any{"x-param-data": map[string]any{"type": "cursor", "cursor": "page.next_cursor"}}

	eventsExtensions := map[string]any{
		"x-res-type": "incremental",
		"x-incremental": map[string]any{
			"strategy":   "head-watermark",
			"items-path": "data.items",
			"watermark": map[string]any{
				"path": "updated_at",
				"type": "datetime",
			},
			"key-paths": []any{"id"},
		},
	}
	if nonIncremental {
		eventsExtensions["x-res-type"] = "one-shot"
	}
	if len(overrides) > 0 {
		eventsExtensions["x-incremental"] = overrides[0]
	}

	paths := []openapi3.NewPathsOption{
		openapi3.WithPath("/events", &openapi3.PathItem{
			Get: &openapi3.Operation{
				Extensions: eventsExtensions,
				Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: cursorParam}},
				Responses:  responseWithJSONSchema("#/components/schemas/root", rootSchema),
			},
		}),
	}
	if includeDownstream {
		eventIDParam := openapi3.NewQueryParameter("eventId").WithRequired(true).WithSchema(openapi3.NewIntegerSchema())
		eventIDParam.Extensions = map[string]any{"x-param-data": map[string]any{"type": "operation", "operation-id": "eventId"}}
		paths = append(paths, openapi3.WithPath("/details", &openapi3.PathItem{
			Get: &openapi3.Operation{
				Extensions: map[string]any{"x-res-type": "one-shot"},
				Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: eventIDParam}},
				Responses: responseWithJSONSchema("#/components/schemas/details", openapi3.NewArraySchema().WithItems(
					openapi3.NewObjectSchema().WithProperty("id", withPK(openapi3.NewIntegerSchema())),
				)),
			},
		}))
	}

	return &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths:   openapi3.NewPaths(paths...),
	}
}

func headWatermarkRootArraySpec(overrides ...map[string]any) *openapi3.T {
	authorSchema := openapi3.NewObjectSchema().WithProperty("date", openapi3.NewStringSchema())
	commitInner := openapi3.NewObjectSchema().
		WithProperty("url", withPK(openapi3.NewStringSchema())).
		WithProperty("author", authorSchema)
	itemSchema := openapi3.NewObjectSchema().
		WithProperty("sha", withPK(openapi3.NewStringSchema())).
		WithProperty("node_id", openapi3.NewStringSchema()).
		WithProperty("commit", commitInner)
	itemSchema.Extensions = map[string]any{"x-table-name": "commit_outer"}
	itemSchema.Properties["commit"].Value.Extensions = map[string]any{"x-table-name": "commit_inner"}

	rootSchema := openapi3.NewArraySchema().WithItems(itemSchema)

	pageParam := openapi3.NewQueryParameter("page").WithSchema(openapi3.NewIntegerSchema())
	pageParam.Extensions = map[string]any{"x-param-data": map[string]any{"type": "offset", "offset": map[string]any{"start": 1, "increment": 1}}}

	extensions := map[string]any{
		"x-res-type": "incremental",
		"x-incremental": map[string]any{
			"strategy":   "head-watermark",
			"items-path": "$",
			"watermark": map[string]any{
				"path": "commit.author.date",
				"type": "datetime",
			},
			"key-paths": []any{"node_id"},
		},
	}
	if len(overrides) > 0 {
		extensions["x-incremental"] = overrides[0]
	}

	return &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/commits", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: extensions,
					Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: pageParam}},
					Responses:  responseWithJSONSchema("#/components/schemas/commits", rootSchema),
				},
			}),
		),
	}
}

func insertedPrimaryKeysForTable(plan *MigrationPlan, tableName, pkColumn string) []string {
	if plan == nil {
		return nil
	}
	var values []string
	for _, op := range plan.Operations {
		insert, ok := op.(InsertRowsOp)
		if !ok || insert.TableName != tableName {
			continue
		}
		for _, row := range insert.Rows {
			for i, column := range row.Columns {
				if column == pkColumn && i < len(row.Values) {
					values = append(values, fmt.Sprintf("%v", row.Values[i].Scalar))
				}
			}
		}
	}
	sort.Strings(values)
	return values
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

func fullSyncTableByName(t *testing.T, plan *FullSyncPlan, tableName string) FullSyncTable {
	t.Helper()

	for _, table := range plan.Tables {
		if table.Name == tableName {
			return table
		}
	}
	t.Fatalf("table %s not found", tableName)
	return FullSyncTable{}
}
