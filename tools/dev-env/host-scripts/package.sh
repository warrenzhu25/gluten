#!/bin/bash

set -ex

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
storage_filepath="$script_dir/../.container/$1"
shift 1

if [ -t 1 ]; then
  docker exec -it $(cat "$storage_filepath") git config --global --add safe.directory /root/incubator-gluten
  docker exec -it $(cat "$storage_filepath") git config --global --add safe.directory /root/incubator-gluten/ep/build-velox/build/velox_ep
  docker exec -it $(cat "$storage_filepath") env PROMPT_ALWAYS_RESPOND=n bash /root/incubator-gluten/dev/package.sh "$@"
else
  docker exec -i $(cat "$storage_filepath") git config --global --add safe.directory /root/incubator-gluten
  docker exec -i $(cat "$storage_filepath") git config --global --add safe.directory /root/incubator-gluten/ep/build-velox/build/velox_ep
  docker exec -i $(cat "$storage_filepath") env PROMPT_ALWAYS_RESPOND=n bash /root/incubator-gluten/dev/package.sh "$@"
fi