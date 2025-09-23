package blobcache

type Schema string

const (
	Schema_NONE Schema = ""
	// Schema_BasicNS is the schema name for the basic namespace.
	Schema_BasicNS        Schema = "blobcache/basicns"
	Schema_BasicContainer Schema = "blobcache/basiccontainer"
)
