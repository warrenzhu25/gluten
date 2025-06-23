#!/bin/bash

set -ex

os_version="Debian12"
java_version="Java17"
env_type="dev"
build_type="Debug"
get_velox="OFF"

while getopts "o:j:rg" opt; do
  case $opt in
    o) os_version="$OPTARG";;
    j) java_version="$OPTARG";;
    r)
      env_type="release"
      build_type="Release"
      ;;
    g)
      get_velox="ON"
      ;;
    *) echo "Invalid option: -$OPTARG"; exit 1;;
  esac
done

if [[ "$get_velox" == "ON" ]]; then
  ./../../ep/build-velox/src/get_velox.sh --get_velox=ON--setup_velox=OFF --velox_remove_local_changes=ON
fi

./host-scripts/package.sh "$env_type-env-$os_version-$java_version" --build_type=$build_type --build_arrow=OFF --get_velox=OFF --setup_velox=OFF --run_setup_script=OFF
