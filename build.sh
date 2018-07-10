#!/bin/bash

set -e

docker build -t sir:$(git log -n 1 --pretty='%h') .
docker tag sir:$(git log -n 1 --pretty='%h') sir:latest
