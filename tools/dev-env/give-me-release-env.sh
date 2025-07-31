#!/bin/bash

set -ex

os_version="Debian12"
volumes=()
java_version="Java17"
get_velox="OFF"
build_type="Release"

while getopts "v:o:j:dg" opt; do
  case $opt in
    v) volumes+=("$OPTARG");;
    o) os_version="$OPTARG";;
    j) java_version="$OPTARG";;
    d) build_type="Debug";;
    g) get_velox="ON";;
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
./../../ep/build-velox/src/get_velox.sh --get_velox=$get_velox --setup_velox=OFF  --velox_remove_local_changes=$get_velox

# Get Spark
./../../ep/build-spark/src/get_spark.sh

# Start docker container
./host-scripts/run.sh -t "release" -o "$os_version" -j $java_version $volume_args

./host-scripts/package.sh "release-env-$os_version-$java_version" ${build_type:+--build_type=$build_type} --build_spark=ON
