// Package migrations embeds the versioned SQL migrations (golang-migrate
// format: NNNNNN_name.up.sql / NNNNNN_name.down.sql).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
