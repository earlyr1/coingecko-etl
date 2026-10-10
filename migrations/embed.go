// Package migrations embeds the goose SQL migrations so the etl binary can
// migrate the database without any files on disk.
package migrations

import "embed"

// FS holds every *.sql migration at its root (e.g. "00001_raw.sql").
//
//go:embed *.sql
var FS embed.FS
