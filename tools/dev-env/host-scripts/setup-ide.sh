#!/bin/bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
storage_filepath="$script_dir/../.container/$1"

docker exec -it $(cat "$storage_filepath") bash /root/incubator-gluten/tools/dev-env/docker-scripts/install-ide.sh
