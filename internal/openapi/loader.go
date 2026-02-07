package openapi

import (
	"context"

	"github.com/getkin/kin-openapi/openapi3"
)

func Load(ctx context.Context, path string) (*openapi3.T, error) {
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true
	return loader.LoadFromFile(path)
}
