#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$SCRIPT_DIR/target/ais-io-1.0-SNAPSHOT-jar-with-dependencies.jar"

# Help
for arg in "$@"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    cat <<'EOF'
Usage: ais-io.sh <input> <output-dir> [OPTIONS]

Ingest AIS data (CSV or NMEA) and write partitioned GeoParquet output.

Arguments:
  <input>          Path to the input file, or a date (yyyy-MM-dd) when --format dma.
                   Supported file formats:
                     - Pre-decoded CSV (.csv)
                     - NMEA 0183 sentence file (.nmea, .aivdm, .txt)
                     - ZIP-compressed versions of the above (.csv.zip, .nmea.zip, .zip)
  <output-dir>     Root directory for output. Created if it does not exist.

Options:
  --format csv|nmea|dma
                   Input format. Auto-detected from the file extension if omitted.
                   For ZIP files the inner entry name is used when the outer name
                   gives no hint (e.g. archive.zip containing data.csv → csv).
                   'dma': first argument is a date (yyyy-MM-dd); data is downloaded
                   from http://aisdata.ais.dk/aisdk-<date>.zip
  --date YYYY-MM-DD
                   Fallback date for NMEA messages that carry no tag-block timestamp.
                   Defaults to today (UTC).
  --limit N        Process at most N lines of data (0 = no limit).
                   Useful for sampling large files during development.
  -h, --help       Show this help message and exit.

Output layout:
  <output-dir>/positions/date=YYYY-MM-DD/h3_r3=<cell>/part-00000.parquet
  <output-dir>/vessels/part-00000.parquet

Examples:
  ais-io.sh data.nmea /tmp/out
  ais-io.sh data.csv /tmp/out --limit 1000
  ais-io.sh archive.zip /tmp/out --format csv --date 2026-03-14
  ais-io.sh data.csv.zip /tmp/out --limit 500
  ais-io.sh 2025-03-02 ./output --format dma

Build the JAR first if not already done:
  cd ais-io && mvn clean package
EOF
    exit 0
  fi
done

if [[ ! -f "$JAR" ]]; then
  echo "JAR not found: $JAR" >&2
  echo "Run: cd ais-io && mvn clean package" >&2
  exit 1
fi

exec java \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.net=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  -jar "$JAR" "$@"
