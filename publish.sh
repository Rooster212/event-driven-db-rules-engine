#!/bin/bash

set -ev

rm -rf ./dist
mkdir ./dist

npx tsc --project tsconfig.build.json

cp package.json ./dist
cp package-lock.json ./dist
cp LICENSE.txt ./dist
cd ./dist

npm publish
