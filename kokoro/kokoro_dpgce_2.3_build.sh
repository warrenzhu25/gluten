#!/bin/bash
set -ex

PATH_TO_CURRENT_DIR=$(dirname $0)
chmod 755 /tmpfs/src/git/incubator-gluten/tools/dev-env/give-me-jar.sh
/bin/bash /tmpfs/src/git/incubator-gluten/tools/dev-env/give-me-jar.sh -v /tmpfs/src/git/velox/:/root/incubator-gluten/ep/build-velox/build/velox_ep/ -o ${OS_VERSION} -j "Java11" -g OFF

gsutil cp /tmpfs/src/git/incubator-gluten/package/target/gluten-velox-bundle-spark3.5_2.12-*.jar gs://nqe-release-jars/native-dpgce-2.3/${OS_VERSION}/
gsutil cp /tmpfs/src/git/incubator-gluten/package/target/thirdparty-lib/gluten-thirdparty-lib-*.jar gs://nqe-release-jars/native-dpgce-2.3/${OS_VERSION}/
