// Package schemas embeds the JSON Schema files of the pipeline so every
// binary carries the schema version it was built with. testdata/ is not
// embedded: fixtures are for tests only.
package schemas

import "embed"

// FS holds every schema file, at paths that mirror the $id
// (https://schemas.cg-etl.internal/<path>). One //go:embed line per
// directory: a pattern that matches no file is a compile error, so a
// directory is added here in the task that creates its first schema.
//
//go:embed common/*.json
//go:embed pipeline/*.json
//go:embed source/coingecko/*.json
//go:embed processed/*.json
var FS embed.FS
