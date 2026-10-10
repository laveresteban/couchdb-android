#!/usr/bin/env bash
# Runs the shared CouchDB conformance specs against this SDK.
# Usage: scripts/conformance.sh [path/to/couchdb-sdk-generator]
# Needs gauge + the java plugin, and CouchDB at COUCHDB_URL.
# GAUGE_TAGS overrides which specs run (see conformance/build.gradle.kts).
set -euo pipefail
cd "$(dirname "$0")/.."
gen="${1:-../couchdb-sdk-generator}"
rm -rf conformance/specs
cp -r "$gen/conformance/specs" conformance/specs

# Gauge passes a run whose scenarios were skipped for missing steps. Treat
# that as a failure so a new spec can't go unimplemented unnoticed.
log="$(mktemp)"
trap 'rm -f "$log"' EXIT
./gradlew :conformance:gauge ${GAUGE_TAGS:+-Ptags="$GAUGE_TAGS"} | tee "$log"
if ! grep -Eq '^Scenarios:.*[^0-9]0 skipped' "$log"; then
  echo "conformance: scenarios were skipped (unimplemented steps?)" >&2
  exit 1
fi
