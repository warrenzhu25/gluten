#!/bin/bash

set -ex

latest_container=$(docker ps --filter "name=$1" --format "{{.Names}}" | sort | tail -n 1)
shift 1

docker exec -it $latest_container git config --global --add safe.directory /root/incubator-gluten
docker exec -it $latest_container git config --global --add safe.directory /root/incubator-gluten/ep/build-velox/build/velox_ep
docker exec -it $latest_container env PROMPT_ALWAYS_RESPOND=n bash /root/incubator-gluten/dev/package.sh "$@"
