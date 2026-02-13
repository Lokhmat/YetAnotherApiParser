package migration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestGenerateMigrations_ChainedFKFlow(t *testing.T) {
	var (
		mu      sync.Mutex
		op2Seen []string
		op3Seen []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/op1":
			_, _ = w.Write([]byte(`[{"id1":10},{"id1":20}]`))
		case "/op2":
			id1 := r.URL.Query().Get("id1")
			mu.Lock()
			op2Seen = append(op2Seen, id1)
			mu.Unlock()
			switch id1 {
			case "10":
				_, _ = w.Write([]byte(`[{"id2":100}]`))
			case "20":
				_, _ = w.Write([]byte(`[{"id2":200}]`))
			default:
				http.Error(w, "unexpected id1", http.StatusBadRequest)
			}
		case "/op3":
			id2 := r.URL.Query().Get("id2")
			mu.Lock()
			op3Seen = append(op3Seen, id2)
			mu.Unlock()
			switch id2 {
			case "100":
				_, _ = w.Write([]byte(`[{"value":"v100"}]`))
			case "200":
				_, _ = w.Write([]byte(`[{"value":"v200"}]`))
			default:
				http.Error(w, "unexpected id2", http.StatusBadRequest)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	spec := buildThreeStepSpec()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	generator := New(10000)
	migrations, err := generator.GenerateMigrations(ctx, spec, server.URL)
	if err != nil {
		t.Fatalf("GenerateMigrations returned error: %v", err)
	}

	if len(migrations) == 0 {
		t.Fatalf("expected non-empty migrations")
	}

	mu.Lock()
	defer mu.Unlock()

	slices.Sort(op2Seen)
	slices.Sort(op3Seen)

	if !slices.Equal(op2Seen, []string{"10", "20"}) {
		t.Fatalf("unexpected op2 calls: got %v, want [10 20]", op2Seen)
	}
	if !slices.Equal(op3Seen, []string{"100", "200"}) {
		t.Fatalf("unexpected op3 calls: got %v, want [100 200]", op3Seen)
	}
}

func TestGenerateMigrations_SourceObjectFeedsChain(t *testing.T) {
	var (
		mu      sync.Mutex
		op2Seen []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/op1":
			// Single object (not array) should still be parsed and used for FK propagation.
			_, _ = w.Write([]byte(`{"id1":77}`))
		case "/op2":
			id1 := r.URL.Query().Get("id1")
			mu.Lock()
			op2Seen = append(op2Seen, id1)
			mu.Unlock()
			if id1 != "77" {
				http.Error(w, "unexpected id1", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`[{"id2":177}]`))
		case "/op3":
			_, _ = w.Write([]byte(`[{"value":"ok"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	spec := buildThreeStepSpec()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	generator := New(10000)
	if _, err := generator.GenerateMigrations(ctx, spec, server.URL); err != nil {
		t.Fatalf("GenerateMigrations returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(op2Seen, []string{"77"}) {
		t.Fatalf("unexpected op2 calls: got %v, want [77]", op2Seen)
	}
}

func buildThreeStepSpec() *openapi3.T {
	id1Field := openapi3.NewIntegerSchema()
	id1Field.Extensions = map[string]any{"x-fk": "id1"}

	id2Field := openapi3.NewIntegerSchema()
	id2Field.Extensions = map[string]any{"x-fk": "id2"}

	op1Schema := openapi3.NewArraySchema().WithItems(
		openapi3.NewObjectSchema().
			WithProperty("id1", id1Field),
	)

	op2Schema := openapi3.NewArraySchema().WithItems(
		openapi3.NewObjectSchema().
			WithProperty("id2", id2Field),
	)

	op3Schema := openapi3.NewArraySchema().WithItems(
		openapi3.NewObjectSchema().
			WithProperty("value", openapi3.NewStringSchema()),
	)

	op1 := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Responses:  responseWithJSONSchema("#/components/schemas/op1", op1Schema),
	}

	paramID1 := openapi3.NewQueryParameter("id1").
		WithRequired(true).
		WithSchema(openapi3.NewIntegerSchema())
	paramID1.Extensions = map[string]any{"x-fk": true}

	op2 := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Parameters: openapi3.Parameters{
			&openapi3.ParameterRef{Value: paramID1},
		},
		Responses: responseWithJSONSchema("#/components/schemas/op2", op2Schema),
	}

	paramID2 := openapi3.NewQueryParameter("id2").
		WithRequired(true).
		WithSchema(openapi3.NewIntegerSchema())
	paramID2.Extensions = map[string]any{"x-fk": true}

	op3 := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Parameters: openapi3.Parameters{
			&openapi3.ParameterRef{Value: paramID2},
		},
		Responses: responseWithJSONSchema("#/components/schemas/op3", op3Schema),
	}

	return &openapi3.T{
		OpenAPI: "3.0.3",
		Info: &openapi3.Info{
			Title:   "test",
			Version: "1.0.0",
		},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/op1", &openapi3.PathItem{Get: op1}),
			openapi3.WithPath("/op2", &openapi3.PathItem{Get: op2}),
			openapi3.WithPath("/op3", &openapi3.PathItem{Get: op3}),
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
