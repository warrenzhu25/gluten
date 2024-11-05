#!/bin/bash

set -ex

os_version="Debian12"
volumes=()
java_version="Java17"

while getopts "v:o:j:d" opt; do
  case $opt in
    v) volumes+=("$OPTARG");;
    o) os_version="$OPTARG";;
    j) java_version="$OPTARG";;
    d) build_type="Debug";;
    *) echo "Invalid option: -$OPTARG"; exit 1;;
  esac
done

volume_args=""
for volume in "${volumes[@]}"; do
  volume_args+="-v $volume "
done

script_dir=$(dirname "$(realpath "$0")")
cd "$script_dir"

# Build docker image
./host-scripts/build.sh "$os_version" "$java_version"

# Get Velox
./../../ep/build-velox/src/get_velox.sh --get_velox=ON --setup_velox=OFF

# Start docker container
./host-scripts/run.sh -t "build" -o $os_version -j $java_version $volume_args

# Ensure remove.sh is executed at the end
trap "./host-scripts/remove.sh build-env-$os_version-$java_version" EXIT

./host-scripts/package.sh "build-env-$os_version-$java_version" ${build_type:+--build_type=$build_type}