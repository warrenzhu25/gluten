#!/bin/bash

set -ex

latest_container=$(docker ps --filter "name=$1" --format "{{.Names}}" | sort | tail -n 1)

docker rm -f $latest_container
