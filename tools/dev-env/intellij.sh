#!/bin/bash

latest_container=$(docker ps --filter "name=dev-env-Debian12-" --format "{{.Names}}" | sort | tail -n 1)

if [ -z "$latest_container" ]; then
  echo "No container running. If this is first time, run give-me-dev-env.sh. If this was run before but container is not running, run start.sh"
else
  docker exec -d -e DISPLAY=$DISPLAY $latest_container /opt/idea/bin/idea
fi
