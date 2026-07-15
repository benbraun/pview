#!/bin/sh
# This script updates the version number for
# the addon based on the current commit timestamp.
# It also pins the addon build to that tag so the
# published addon image contains exactly the tagged code.
TAG_NAME=${TAG_NAME:-$(git -c "core.abbrev=8" show -s "--format=%cd-%h" "--date=format:%Y.%m.%d")}
sed -i "s/version:.*/version: \"$TAG_NAME\"/" addon/config.yaml
sed -i "s/PVIEW_REF:.*/PVIEW_REF: \"$TAG_NAME\"/" addon/build.yaml
