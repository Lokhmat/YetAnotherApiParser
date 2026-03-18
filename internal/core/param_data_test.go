package core

import (
	"context"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestGetParamDataSpecsModes(t *testing.T) {
	service := NewService(nil)

	operationParam := openapi3.NewQueryParameter("publicationId").WithSchema(openapi3.NewIntegerSchema())
	operationParam.Extensions = map[string]any{
		"x-param-data": map[string]any{
			"type":         "operation",
			"operation-id": "pub_id",
			"filter":       map[string]any{"op": "gte", "value": 2},
		},
	}

	valuesParam := openapi3.NewQueryParameter("symbols").WithSchema(openapi3.NewStringSchema())
	valuesParam.Extensions = map[string]any{
		"x-param-data": map[string]any{
			"type":   "values",
			"values": []any{"BTCUSDT", "ETHUSDT"},
		},
	}

	cursorParam := openapi3.NewQueryParameter("cursor").WithSchema(openapi3.NewStringSchema())
	cursorParam.Extensions = map[string]any{
		"x-param-data": map[string]any{
			"type":   "cursor",
			"cursor": "next.cursor",
		},
	}

	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{
			&openapi3.ParameterRef{Value: operationParam},
			&openapi3.ParameterRef{Value: valuesParam},
			&openapi3.ParameterRef{Value: cursorParam},
		},
	}

	specs, paginationSpec, err := service.getParamDataSpecs("/test", op)
	if err != nil {
		t.Fatalf("getParamDataSpecs returned error: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 combination specs, got %d", len(specs))
	}
	if specs[0].ParamName != "publicationId" || specs[0].OperationID != "pub_id" || specs[0].Filter == nil {
		t.Fatalf("unexpected operation spec: %+v", specs[0])
	}
	if specs[1].ParamName != "symbols" || len(specs[1].Values) != 2 {
		t.Fatalf("unexpected values spec: %+v", specs[1])
	}
	if paginationSpec == nil || paginationSpec.ParamName != "cursor" || paginationSpec.Type != paramDataTypeCursor {
		t.Fatalf("unexpected pagination spec: %+v", paginationSpec)
	}
}

func TestGetParamDataSpecsRejectsLegacyXFK(t *testing.T) {
	service := NewService(nil)
	p := openapi3.NewQueryParameter("publicationId").WithSchema(openapi3.NewIntegerSchema())
	p.Extensions = map[string]any{"x-fk": "publicationId"}
	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: p}},
	}

	_, _, err := service.getParamDataSpecs("/test", op)
	if err == nil || !strings.Contains(err.Error(), "x-fk is no longer supported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetParamDataSpecsRejectsMultiplePaginationParams(t *testing.T) {
	service := NewService(nil)

	cursorParam := openapi3.NewQueryParameter("cursor").WithSchema(openapi3.NewStringSchema())
	cursorParam.Extensions = map[string]any{"x-param-data": map[string]any{"type": "cursor", "cursor": "next.cursor"}}

	offsetParam := openapi3.NewQueryParameter("offset").WithSchema(openapi3.NewIntegerSchema())
	offsetParam.Extensions = map[string]any{"x-param-data": map[string]any{"type": "offset", "offset": map[string]any{"start": 0, "increment": 1}}}

	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{
			&openapi3.ParameterRef{Value: cursorParam},
			&openapi3.ParameterRef{Value: offsetParam},
		},
	}

	_, _, err := service.getParamDataSpecs("/test", op)
	if err == nil || !strings.Contains(err.Error(), "multiple pagination parameters") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateResponseDataHintsRejectsLegacyXFK(t *testing.T) {
	field := openapi3.NewIntegerSchema()
	field.Extensions = map[string]any{"x-fk": "id1"}

	op := &openapi3.Operation{
		Responses: responseWithJSONSchema("#/components/schemas/test", openapi3.NewArraySchema().WithItems(
			openapi3.NewObjectSchema().WithProperty("id", field),
		)),
	}

	err := validateResponseDataHints("/test", op)
	if err == nil || !strings.Contains(err.Error(), "x-fk is no longer supported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyParamFilterInWithArrayValues(t *testing.T) {
	values := []interface{}{
		[]interface{}{"BTCUSDT", "BNBBTC"},
		[]interface{}{"ETHUSDT", "BNBUSDT"},
	}
	filter := &ParamFilterSpec{
		Op:     filterOpIn,
		Values: []interface{}{[]interface{}{"BTCUSDT", "BNBBTC"}},
	}

	filtered, err := applyParamFilter(values, filter)
	if err != nil {
		t.Fatalf("applyParamFilter returned error: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected 1 filtered value, got %+v", filtered)
	}
}

func TestBuildEffectiveParamValueListsOperationAndValues(t *testing.T) {
	service := NewService(nil)
	specs := []ParamDataSpec{
		{
			ParamName:   "a",
			Type:        paramDataTypeOperation,
			OperationID: "a",
			Filter:      &ParamFilterSpec{Op: filterOpGTE, Value: 2},
		},
		{
			ParamName: "b",
			Type:      paramDataTypeValues,
			Values:    []interface{}{"2025-01-01T00:00:00Z"},
		},
	}

	fetched := map[string][]interface{}{
		"a": {1, 2, 3},
	}
	lists, ready, err := service.buildEffectiveParamValueLists("/test", specs, fetched)
	if err != nil {
		t.Fatalf("buildEffectiveParamValueLists returned error: %v", err)
	}
	if !ready || len(lists) != 2 || len(lists[0]) != 2 || len(lists[1]) != 1 {
		t.Fatalf("unexpected lists: ready=%v lists=%+v", ready, lists)
	}
}

func TestBuildEffectiveParamValueListsWaitsForOperationValues(t *testing.T) {
	service := NewService(nil)
	specs := []ParamDataSpec{{
		ParamName:   "a",
		Type:        paramDataTypeOperation,
		OperationID: "a",
		Filter:      &ParamFilterSpec{Op: filterOpIn, Values: []interface{}{999}},
	}}

	lists, ready, err := service.buildEffectiveParamValueLists("/test", specs, map[string][]interface{}{
		"a": {1, 2, 3},
	})
	if err != nil {
		t.Fatalf("buildEffectiveParamValueLists returned error: %v", err)
	}
	if ready {
		t.Fatalf("expected ready=false, got true with lists=%+v", lists)
	}
}

func TestGeneratePlanValuesOnlyParam(t *testing.T) {
	api := &fakeAPIConnector{
		responses: map[string]func(req FetchRequest) ([]byte, error){
			"/values": func(req FetchRequest) ([]byte, error) {
				return []byte(`[{"id":1}]`), nil
			},
		},
	}

	param := openapi3.NewQueryParameter("symbol").WithRequired(true).WithSchema(openapi3.NewStringSchema())
	param.Extensions = map[string]any{"x-param-data": map[string]any{"type": "values", "values": []any{"BTCUSDT", "ETHUSDT"}}}

	op := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: param}},
		Responses:  responseWithJSONSchema("#/components/schemas/values", openapi3.NewArraySchema().WithItems(openapi3.NewObjectSchema().WithProperty("id", withPK(openapi3.NewIntegerSchema())))),
	}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info:    &openapi3.Info{Title: "test", Version: "1.0.0"},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/values", &openapi3.PathItem{Get: op}),
		),
	}

	service := NewService(api)
	if _, err := service.GeneratePlan(context.Background(), spec, "https://example.com"); err != nil {
		t.Fatalf("GeneratePlan returned error: %v", err)
	}
	if len(api.seen["/values"]) != 2 {
		t.Fatalf("expected 2 static value requests, got %v", api.seen["/values"])
	}
}
