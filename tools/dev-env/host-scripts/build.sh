#!/bin/bash

set -ex

DIR="$(dirname "$(realpath "$0")")/$1/$2"

if [ ! -f "$DIR/Dockerfile" ]; then
    echo "OS $1 and Java Version $2 not supported"
    exit 1
fi

# Run the Docker build command
docker build -t "env:$1-$2" "$DIR"
