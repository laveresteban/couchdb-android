#!/usr/bin/env bash
# Runs the shared CouchDB conformance specs against this SDK.
# Usage: scripts/conformance.sh [path/to/couchdb-sdk-generator]
# Needs gauge + the java plugin, and CouchDB at COUCHDB_URL.
set -euo pipefail
cd "$(dirname "$0")/.."
gen="${1:-../couchdb-sdk-generator}"
rm -rf conformance/specs
cp -r "$gen/conformance/specs" conformance/specs
./gradlew :conformance:gauge ${GAUGE_TAGS:+-Ptags="$GAUGE_TAGS"}
