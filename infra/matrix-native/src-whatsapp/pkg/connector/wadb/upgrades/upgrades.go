package upgrades

import (
	"embed"

	"go.mau.fi/util/dbutil"
)

//go:embed *.sql
var rawUpgrades embed.FS

// util v0.9.12 replaced the mutable UpgradeTable.RegisterFS with the
// BuildUpgradeTable()…Finish() builder (see src-signal for the reference).
var Table = dbutil.BuildUpgradeTable().
	WithFS(rawUpgrades).
	Finish()
