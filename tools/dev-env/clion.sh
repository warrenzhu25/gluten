#!/bin/bash

os_version="Debian12"
java_version="Java17"
env_type="dev"

while getopts "o:j:r" opt; do
  case $opt in
    o) os_version="$OPTARG";;
    j) java_version="$OPTARG";;
    r) env_type="release";;
    *) echo "Invalid option: -$OPTARG"; exit 1;;
  esac
done

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
storage_filepath="$script_dir/.container/$env_type-env-$os_version-$java_version"

docker exec -d -e DISPLAY=$DISPLAY $(cat "$storage_filepath") /opt/clion/bin/clion
