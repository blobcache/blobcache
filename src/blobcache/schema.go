package blobcache

import "encoding/json"

type SchemaName string

const Schema_NONE SchemaName = ""

type SchemaSpec struct {
	// Name is the name of the schema.
	Name SchemaName `json:"name"`
	// Params are the parameters for the schema.
	Params json.RawMessage `json:"params,omitempty"`
}

func (s SchemaSpec) Marshal(out []byte) []byte {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (s *SchemaSpec) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}
