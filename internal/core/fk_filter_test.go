package core

import (
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestGetFKParamSpecsBoolAndObject(t *testing.T) {
	service := NewService(nil)

	p1 := openapi3.NewQueryParameter("id").WithSchema(openapi3.NewIntegerSchema())
	p1.Extensions = map[string]any{"x-fk": true}

	p2 := openapi3.NewQueryParameter("publicationId").WithSchema(openapi3.NewIntegerSchema())
	p2.Extensions = map[string]any{
		"x-fk": map[string]any{
			"id":     "pub_id",
			"values": []any{1, 2, 3},
			"filter": map[string]any{"op": "gte", "value": 2},
		},
	}

	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{
			&openapi3.ParameterRef{Value: p1},
			&openapi3.ParameterRef{Value: p2},
		},
	}

	specs, err := service.getFKParamSpecs("/test", op)
	if err != nil {
		t.Fatalf("getFKParamSpecs returned error: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}
	if specs[0].ParamName != "id" || specs[0].DependencyKey != "id" {
		t.Fatalf("unexpected bool x-fk parsing: %+v", specs[0])
	}
	if specs[1].ParamName != "publicationId" || specs[1].DependencyKey != "pub_id" {
		t.Fatalf("unexpected object x-fk parsing: %+v", specs[1])
	}
}

func TestGetFKParamSpecsRejectsStringFormat(t *testing.T) {
	service := NewService(nil)
	p := openapi3.NewQueryParameter("publicationId").WithSchema(openapi3.NewIntegerSchema())
	p.Extensions = map[string]any{"x-fk": "publicationId"}
	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{&openapi3.ParameterRef{Value: p}},
	}

	_, err := service.getFKParamSpecs("/test", op)
	if err == nil || !strings.Contains(err.Error(), "deprecated") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyFKFilterInWithArrayValues(t *testing.T) {
	values := []interface{}{
		[]interface{}{"BTCUSDT", "BNBBTC"},
		[]interface{}{"ETHUSDT", "BNBUSDT"},
	}
	filter := &FKFilterSpec{
		Op:     filterOpIn,
		Values: []interface{}{[]interface{}{"BTCUSDT", "BNBBTC"}},
	}

	filtered, err := applyFKFilter(values, filter)
	if err != nil {
		t.Fatalf("applyFKFilter returned error: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected 1 filtered value, got %+v", filtered)
	}
}

func TestBuildEffectiveFKValueListsSeedAndFilter(t *testing.T) {
	service := NewService(nil)
	specs := []FKParamSpec{
		{
			ParamName:     "a",
			DependencyKey: "a",
			SeedValues:    []interface{}{1, 2},
			Filter:        &FKFilterSpec{Op: filterOpGTE, Value: 2},
		},
		{
			ParamName:     "b",
			DependencyKey: "b",
			SeedValues:    []interface{}{"2025-01-01T00:00:00Z"},
			Filter:        &FKFilterSpec{Op: filterOpLT, Value: "2026-01-01T00:00:00Z"},
		},
	}

	fetched := map[string][]interface{}{
		"a": {3},
		"b": {"2027-01-01T00:00:00Z"},
	}
	lists, ready, err := service.buildEffectiveFKValueLists("/test", specs, fetched)
	if err != nil {
		t.Fatalf("buildEffectiveFKValueLists returned error: %v", err)
	}
	if !ready || len(lists) != 2 || len(lists[0]) != 2 || len(lists[1]) != 1 {
		t.Fatalf("unexpected lists: ready=%v lists=%+v", ready, lists)
	}
}

func TestBuildEffectiveFKValueListsEmptyAfterFilter(t *testing.T) {
	service := NewService(nil)
	specs := []FKParamSpec{{
		ParamName:     "a",
		DependencyKey: "a",
		Filter:        &FKFilterSpec{Op: filterOpIn, Values: []interface{}{999}},
	}}

	lists, ready, err := service.buildEffectiveFKValueLists("/test", specs, map[string][]interface{}{
		"a": {1, 2, 3},
	})
	if err != nil {
		t.Fatalf("buildEffectiveFKValueLists returned error: %v", err)
	}
	if ready {
		t.Fatalf("expected ready=false, got true with lists=%+v", lists)
	}
}
