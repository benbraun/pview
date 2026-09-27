#!/bin/sh
set -eu
cd "$(git rev-parse --show-toplevel)"
TAG_NAME=${TAG_NAME:-$(git -c "core.abbrev=8" show -s "--format=%cd-%h" "--date=format:%Y.%m.%d")}
case "$TAG_NAME" in
  ''|*[!a-zA-Z0-9._-]*) echo "Invalid release tag: $TAG_NAME" >&2; exit 1 ;;
esac
# A temporary file keeps this portable across BSD and GNU systems.
tmp=$(mktemp addon/config.yaml.XXXXXX)
trap 'rm -f "$tmp"' EXIT HUP INT TERM
awk -v version="$TAG_NAME" '/^version:/ {$0 = "version: \"" version "\""} {print}' addon/config.yaml > "$tmp"
cat "$tmp" > addon/config.yaml
