#!/bin/sh
set -eu
cd "$(git rev-parse --show-toplevel)"
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Commit tracked changes before preparing a release" >&2
  exit 1
fi
TAG_NAME=${TAG_NAME:-$(git -c "core.abbrev=8" show -s "--format=%cd-%h" "--date=format:%Y.%m.%d")}
export TAG_NAME
git check-ref-format "refs/tags/$TAG_NAME"
if git show-ref --verify --quiet "refs/tags/$TAG_NAME"; then
  echo "Release tag already exists: $TAG_NAME" >&2
  exit 1
fi
./scripts/apply-tag.sh
git add addon/config.yaml
git commit -m "Release $TAG_NAME"
git tag -a "$TAG_NAME" -m "Release $TAG_NAME"
