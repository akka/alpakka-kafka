#!/bin/bash

VERSION=$1
if [ -z $VERSION ]
then
  echo specify the version name to be released, eg. 1.0.0
else
  sed -e 's/\$VERSION\$/'$VERSION'/g' docs/release-train-issue-template.md > /tmp/release-$VERSION.md
  echo Created $(hub issue create -F /tmp/release-$VERSION.md --milestone $VERSION --browse)
fi
