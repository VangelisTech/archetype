# Installed consumer migration fixtures

`migration-inputs.tar.gz` contains only the selected actual Gateway authentication
and tenant hosting adapter, Holocron artifact adapter, and their focused contracts.
It excludes unrelated Holocron memory/X0/Lemmalog tools, world-library/domain
modules, owner state, private corpus, credentials and native binaries.

`migration-inputs.json` records every selected member hash, original input hash
where available and the explicit packaging scope. Holocron fixture metadata
omits unrelated memory CLI and data-file contributions; it does not claim a
complete or published Holocron release. Adapter source bytes match the actual
isolated consumer port. Complete input snapshots stay local.

The gate builds the selected adapter wheels, installs them with exact product
artifacts, copies only tests into a neutral fixture directory and verifies loaded
product source/wheel/origin hashes. Gateway uses synthetic real JWT signatures,
the real native C ABI, HTTP and official MCP SDK. Holocron covers artifact-only
contexts, cold media facts, exact retry identities and binding durability.
The earlier provenance and provider-replay API remains on its separately
preserved, explicitly matched 0.6 source line.
