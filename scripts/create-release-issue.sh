#!/bin/bash

VERSION=$1
if [ -z $VERSION ]
then
  echo specify the version name to be released, eg. 2.2.0
else
  sed -e 's/\$VERSION\$/'$VERSION'/g' docs/release-train-issue-template.md > /tmp/release-$VERSION.md
  echo Created $(gh issue create --title 'Release Alpakka Kafka' --body-file /tmp/release-$VERSION.md --milestone $VERSION --web)
fi
