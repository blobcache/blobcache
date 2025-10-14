package blobcache

import "encoding/json"

type SchemaName string

const (
	Schema_NONE SchemaName = ""
	// Schema_BasicNS is the schema name for the basic namespace.
	Schema_BasicNS        SchemaName = "blobcache/basicns"
	Schema_BasicContainer SchemaName = "blobcache/basiccontainer"

	Schema_MerkleLog SchemaName = "merklelog"
)

type SchemaSpec struct {
	// Name is the name of the schema.
	Name SchemaName `json:"name"`
	// Params are the parameters for the schema.
	Params json.RawMessage `json:"params"`
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
