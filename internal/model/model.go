package model

type DTO struct {
	Name       string
	Fields     []Field
	Extensions map[string]any
	Metadata   map[string]any
}

type Field struct {
	Name       string
	JSONName   string
	Type       string
	Required   bool
	Extensions map[string]any
	Metadata   map[string]any
}

type Handler struct {
	Name        string
	Method      string
	Path        string
	Summary     string
	Description string
	Parameters  []Parameter
	ResponseDTO string
	Extensions  map[string]any
	Metadata    map[string]any
}

type Parameter struct {
	Name       string
	In         string
	Type       string
	Required   bool
	Extensions map[string]any
	Metadata   map[string]any
}
