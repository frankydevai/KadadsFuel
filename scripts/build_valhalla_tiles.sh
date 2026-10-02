#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bash scripts/build_valhalla_tiles.sh /path/to/us-latest.osm.pbf" >&2
  exit 2
fi

PBF_PATH="$1"
CONFIG_PATH="${VALHALLA_CONFIG:-valhalla/valhalla.json}"
TILE_DIR="valhalla/tiles"
TILE_TAR="valhalla/tiles.tar"

if [[ ! -f "$PBF_PATH" ]]; then
  echo "PBF file not found: $PBF_PATH" >&2
  exit 2
fi

if ! command -v valhalla_build_tiles >/dev/null 2>&1; then
  echo "valhalla_build_tiles is not installed or not on PATH." >&2
  echo "Install Valhalla tooling, then rerun this script." >&2
  exit 127
fi

mkdir -p "$TILE_DIR"

echo "Building Valhalla truck-routing tiles..."
echo "  config: $CONFIG_PATH"
echo "  input:  $PBF_PATH"
echo "  out:    $TILE_DIR"
valhalla_build_tiles -c "$CONFIG_PATH" "$PBF_PATH"

if command -v valhalla_build_extract >/dev/null 2>&1; then
  echo "Packing tile extract: $TILE_TAR"
  valhalla_build_extract -c "$CONFIG_PATH" -v
else
  echo "valhalla_build_extract not found; leaving unpacked tiles in $TILE_DIR"
fi

echo "Done. Configure these tiles on the separately managed Valhalla routing server."
