#!/bin/bash

latest_container=$(docker ps --all --filter "name=dev-env-Debian12-" --format "{{.Names}}" | sort | tail -n 1)

docker start $latest_container