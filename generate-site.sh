#!/bin/bash
set -e
mvn site
cd ../davidmoten.github.io
git pull
mkdir -p rxjava-file
cp -r ../rxjava-file/target/site/* rxjava-file/
git add .
git commit -am "update site reports"
git push
