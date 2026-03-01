package migration

import (
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestGetFKParamSpecs_BoolAndObject(t *testing.T) {
	g := New(1000)

	p1 := openapi3.NewQueryParameter("id").WithSchema(openapi3.NewIntegerSchema())
	p1.Extensions = map[string]any{"x-fk": true}

	p2 := openapi3.NewQueryParameter("publicationId").WithSchema(openapi3.NewIntegerSchema())
	p2.Extensions = map[string]any{
		"x-fk": map[string]any{
			"id":     "pub_id",
			"values": []any{1, 2, 3},
			"filter": map[string]any{
				"op":    "gte",
				"value": 2,
			},
		},
	}

	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{
			&openapi3.ParameterRef{Value: p1},
			&openapi3.ParameterRef{Value: p2},
		},
	}

	specs, err := g.getFKParamSpecs("/test", op)
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
	if len(specs[1].SeedValues) != 3 {
		t.Fatalf("expected seed values, got %+v", specs[1].SeedValues)
	}
	if specs[1].Filter == nil || specs[1].Filter.Op != "gte" {
		t.Fatalf("expected parsed filter, got %+v", specs[1].Filter)
	}
}

func TestGetFKParamSpecs_ObjectWithArraySeedValues(t *testing.T) {
	g := New(1000)

	p := openapi3.NewQueryParameter("symbols").WithSchema(openapi3.NewStringSchema())
	p.Extensions = map[string]any{
		"x-fk": map[string]any{
			"values": []any{
				[]any{"BTCUSDT", "BNBBTC"},
				[]any{"ETHUSDT", "BNBUSDT"},
			},
		},
	}
	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{
			&openapi3.ParameterRef{Value: p},
		},
	}

	specs, err := g.getFKParamSpecs("/test", op)
	if err != nil {
		t.Fatalf("getFKParamSpecs returned error: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}
	if len(specs[0].SeedValues) != 2 {
		t.Fatalf("expected 2 seed values, got %+v", specs[0].SeedValues)
	}
	if _, ok := specs[0].SeedValues[0].([]interface{}); !ok {
		t.Fatalf("expected first seed value to be array, got %T", specs[0].SeedValues[0])
	}
}

func TestGetFKParamSpecs_RejectsStringFormat(t *testing.T) {
	g := New(1000)

	p := openapi3.NewQueryParameter("publicationId").WithSchema(openapi3.NewIntegerSchema())
	p.Extensions = map[string]any{"x-fk": "publicationId"}
	op := &openapi3.Operation{
		Parameters: openapi3.Parameters{
			&openapi3.ParameterRef{Value: p},
		},
	}

	_, err := g.getFKParamSpecs("/test", op)
	if err == nil {
		t.Fatalf("expected error for deprecated string format")
	}
	if !strings.Contains(err.Error(), "deprecated") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyFKFilter_InWithArrayValues(t *testing.T) {
	values := []interface{}{
		[]interface{}{"BTCUSDT", "BNBBTC"},
		[]interface{}{"ETHUSDT", "BNBUSDT"},
	}
	filter := &FKFilterSpec{
		Op: filterOpIn,
		Values: []interface{}{
			[]interface{}{"BTCUSDT", "BNBBTC"},
		},
	}

	filtered, err := applyFKFilter(values, filter)
	if err != nil {
		t.Fatalf("applyFKFilter returned error: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected 1 filtered value, got %+v", filtered)
	}
}

func TestBuildEffectiveFKValueLists_SeedAndFilter(t *testing.T) {
	g := New(1000)
	specs := []FKParamSpec{
		{
			ParamName:     "a",
			DependencyKey: "a",
			SeedValues:    []interface{}{1, 2},
			Filter: &FKFilterSpec{
				Op:    filterOpGTE,
				Value: 2,
			},
		},
		{
			ParamName:     "b",
			DependencyKey: "b",
			SeedValues:    []interface{}{"2025-01-01T00:00:00Z"},
			Filter: &FKFilterSpec{
				Op:    filterOpLT,
				Value: "2026-01-01T00:00:00Z",
			},
		},
	}

	fetched := map[string][]interface{}{
		"a": []interface{}{3},
		"b": []interface{}{"2027-01-01T00:00:00Z"},
	}

	lists, ready, err := g.buildEffectiveFKValueLists("/test", specs, fetched)
	if err != nil {
		t.Fatalf("buildEffectiveFKValueLists returned error: %v", err)
	}
	if !ready {
		t.Fatalf("expected ready=true")
	}
	if len(lists) != 2 {
		t.Fatalf("expected 2 lists, got %d", len(lists))
	}
	if len(lists[0]) != 2 { // [2,3]
		t.Fatalf("unexpected numeric list: %+v", lists[0])
	}
	if len(lists[1]) != 1 { // 2025-01-01 only
		t.Fatalf("unexpected datetime list: %+v", lists[1])
	}
}

func TestBuildEffectiveFKValueLists_EmptyAfterFilter(t *testing.T) {
	g := New(1000)
	specs := []FKParamSpec{
		{
			ParamName:     "a",
			DependencyKey: "a",
			Filter: &FKFilterSpec{
				Op:     filterOpIn,
				Values: []interface{}{999},
			},
		},
	}

	lists, ready, err := g.buildEffectiveFKValueLists("/test", specs, map[string][]interface{}{
		"a": []interface{}{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("buildEffectiveFKValueLists returned error: %v", err)
	}
	if ready {
		t.Fatalf("expected ready=false, got true with lists=%+v", lists)
	}
}
