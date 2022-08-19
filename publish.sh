#!/bin/bash

set -ev

rm -rf ./dist
mkdir ./dist

npx tsc --project tsconfig.build.json

cp package.json ./dist
cp package-lock.json ./dist
cp LICENSE.txt ./dist
cd ./dist

if [ "$1" = "dry-run" ]; then
  echo "================================================="
  echo "Dry-run of publish"
  echo "================================================="
  npm publish --dry-run
  echo "================================================="
else
  echo "Publishing package"
  npm publish --dry-run
fi

