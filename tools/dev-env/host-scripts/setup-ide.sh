#!/bin/bash

latest_container=$(docker ps --filter "name=$1" --format "{{.Names}}" | sort | tail -n 1)

docker exec -it $latest_container bash /root/incubator-gluten/tools/dev-env/docker-scripts/install-ide.sh
