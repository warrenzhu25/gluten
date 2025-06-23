#!/bin/bash

set -ex

os_version=""
env_type="build"
volumes=()
java_version=""

timestamp=$(date +%Y%m%d%H%M%S)

while getopts "o:v:t:j:" opt; do
  case $opt in
    o) os_version="$OPTARG";;
    v) volumes+=("$OPTARG");;
    t) env_type="$OPTARG";;
    j) java_version="$OPTARG";;
    *) echo "Invalid option: -$OPTARG"; exit 1;;
  esac
done

if [ -z "$os_version" ]; then
  echo "OS version is required. Use -o to specify it."
  exit 1
fi

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
mkdir -p "$script_dir/../.container"
container_name_with_timestamp="$env_type-env-$os_version-$java_version-$timestamp"
storage_filename="$env_type-env-$os_version-$java_version"
storage_filepath="$script_dir/../.container/$storage_filename"

echo "$container_name_with_timestamp" > "$storage_filepath"

mandatory_volumes=(
  "-v $HOME/.Xauthority:/root/.Xauthority"
  "-v /tmp/.X11-unix:/tmp/.X11-unix"
  "-v $(realpath $(dirname "$0"))/../../..:/root/incubator-gluten"
)

volume_args=()
for volume in "${mandatory_volumes[@]}"; do
  volume_args+=("$volume")
done

for volume in "${volumes[@]}"; do
  volume_args+=("-v" "$volume")
done

# Run the Docker container using the name from the file inside the .container directory
docker run -d -e DISPLAY="${DISPLAY}" \
  ${volume_args[@]} \
  --network=host --privileged \
  --name "$(cat "$storage_filepath")" "env:$os_version-$java_version"