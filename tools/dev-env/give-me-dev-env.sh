#!/bin/bash

set -ex

os_version="Debian12"
volumes=()
build_type="Debug"
java_version="Java17"
spark_version="3.5.3"
scala_version="2.12"

while getopts "v:j:" opt; do
  case $opt in
    v) volumes+=("$OPTARG");;
    j) java_version="$OPTARG";;
    c) scala_version="$OPTARG";;
    s) spark_version="$OPTARG";;
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
./../../ep/build-velox/src/get_velox.sh --get_velox=ON --setup_velox=OFF  --velox_remove_local_changes=ON

# Get Spark dependencies
source ./../../ep/build-spark/src/get_spark.sh
fetch_spark_deps ${spark_version} ${scala_version}

# Start docker container
./host-scripts/run.sh -t "dev" -o "$os_version" -j $java_version $volume_args

./host-scripts/package.sh "dev-env-$os_version-$java_version" ${build_type:+--build_type=$build_type}

./host-scripts/setup-ide.sh "dev-env-$os_version-$java_version" $os_version
