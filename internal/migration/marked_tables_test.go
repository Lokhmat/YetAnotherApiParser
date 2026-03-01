package migration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestBuildOperationExtractionPlan_RelationShapes(t *testing.T) {
	g := New(1000)

	usersSchema := openapi3.NewObjectSchema().
		WithProperty("id", withPK(openapi3.NewIntegerSchema())).
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

	root := openapi3.NewObjectSchema().
		WithProperty("usersPayload", usersSchema).
		WithProperty("ordersPayload", ordersSchema)

	op := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Responses: responseWithJSONSchema(
			"#/components/schemas/root",
			root,
		),
	}

	plan, err := g.buildOperationExtractionPlan(op)
	if err != nil {
		t.Fatalf("buildOperationExtractionPlan returned error: %v", err)
	}
	if !plan.HasMarks {
		t.Fatalf("expected plan.HasMarks")
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
		t.Fatalf("expected users->settings direct_ref, got %v", relations["users->settings"])
	}
	if relations["users->phones"] != relationDirectRef {
		t.Fatalf("expected users->phones direct_ref, got %v", relations["users->phones"])
	}
	if relations["orders->customers"] != relationDirectRef {
		t.Fatalf("expected orders->customers direct_ref, got %v", relations["orders->customers"])
	}
	if relations["orders->tags"] != relationLinkTable {
		t.Fatalf("expected orders->tags link_table, got %v", relations["orders->tags"])
	}
	if !joinTables["orders_tags_link"] {
		t.Fatalf("expected orders_tags_link join table")
	}
}

func TestBuildOperationExtractionPlan_RequiresSinglePK(t *testing.T) {
	g := New(1000)

	bad := openapi3.NewObjectSchema().
		WithProperty("id", openapi3.NewIntegerSchema())
	bad.Extensions = map[string]any{"x-table-name": "bad_table"}

	op := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Responses:  responseWithJSONSchema("#/components/schemas/bad", bad),
	}

	_, err := g.buildOperationExtractionPlan(op)
	if err == nil {
		t.Fatalf("expected error for missing x-pk")
	}
	if !strings.Contains(err.Error(), "exactly one x-pk") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenerateMigrations_MarkedMode_RecursiveInserts(t *testing.T) {
	var calls int
	server := newStaticJSONServer(t, `{
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
	}`, &calls)
	defer server.Close()

	g := New(10000)
	spec := markedSpec()
	migs, err := g.GenerateMigrations(context.Background(), spec, server.URL)
	if err != nil {
		t.Fatalf("GenerateMigrations returned error: %v", err)
	}
	if calls == 0 {
		t.Fatalf("expected endpoint to be called")
	}

	all := strings.Join(migs, "\n")
	assertContains(t, all, "CREATE TABLE users")
	assertContains(t, all, "CREATE TABLE settings")
	assertContains(t, all, "CREATE TABLE phones")
	assertContains(t, all, "CREATE TABLE orders")
	assertContains(t, all, "CREATE TABLE customers")
	assertContains(t, all, "CREATE TABLE tags")
	assertContains(t, all, "CREATE TABLE orders_tags_link")

	assertContains(t, all, "INSERT INTO settings")
	assertContains(t, all, "INSERT INTO phones")
	assertContains(t, all, "INSERT INTO users")
	assertContains(t, all, "ARRAY[10, 11]::INTEGER[]")
	assertContains(t, all, "INSERT INTO orders_tags_link")
}

func markedSpec() *openapi3.T {
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

	root := openapi3.NewObjectSchema().
		WithProperty("usersPayload", usersSchema).
		WithProperty("ordersPayload", ordersSchema)

	op := &openapi3.Operation{
		Extensions: map[string]any{"x-res-type": "one-shot"},
		Responses:  responseWithJSONSchema("#/components/schemas/root", root),
	}

	return &openapi3.T{
		OpenAPI: "3.0.3",
		Info: &openapi3.Info{
			Title:   "test",
			Version: "1.0.0",
		},
		Paths: openapi3.NewPaths(
			openapi3.WithPath("/marked", &openapi3.PathItem{Get: op}),
		),
	}
}

func withPK(schema *openapi3.Schema) *openapi3.Schema {
	if schema.Extensions == nil {
		schema.Extensions = map[string]any{}
	}
	schema.Extensions["x-pk"] = true
	return schema
}

func assertContains(t *testing.T, s, needle string) {
	t.Helper()
	if !strings.Contains(s, needle) {
		t.Fatalf("expected to contain %q\nactual:\n%s", needle, s)
	}
}

func newStaticJSONServer(t *testing.T, payload string, calls *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		(*calls)++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
}
