#!/bin/bash

set -ex

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
storage_filepath="$script_dir/../.container/$1"

docker rm -f $(cat "$storage_filepath")
rm -f $storage_filepath
