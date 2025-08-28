#!/bin/bash
set -ex

PATH_TO_CURRENT_DIR=$(dirname $0)
source "${PATH_TO_CURRENT_DIR}"/jar_util.sh

echo "Running as " "$(whoami)"

download_maven

JAVA_VERSION="Java11"
SPARK_VERSION="3.5.3"
SCALA_VERSION="2.12"

build_jars "${OS_VERSION}" "${JAVA_VERSION}" "${SPARK_VERSION}" "${SCALA_VERSION}"

TODAY=$(date +%Y%m%d-%H%M)
VERSION="1.4.0-${TODAY}"
DESCRIPTION="Gluten 1.4 with Spark ${SPARK_VERSION} jars for dpgce-2.3"
VELOX_BUNDLE_JAR="gluten-velox-bundle-spark${SPARK_VERSION}_${SCALA_VERSION}-linux_amd64-1.4.0.jar"

# Upload to Dataproc Artifact Registry
if [ "$PUBLISH_JARS_TO_AR" == "yes" ]; then
  cp "${GLUTEN_DIR}"/package/target/"${VELOX_BUNDLE_JAR}" "${BASE_DIR}"

  cd "${GLUTEN_DIR}"
  upload_jar "dpgce-2.3-velox-bundle-Debian12" "${VERSION}" "${DESCRIPTION}" "${BASE_DIR}/${VELOX_BUNDLE_JAR}"
fi

# Upload to GCS Location
if [ "$PUBLISH_JARS_TO_GCS" == "yes" ]; then
  gsutil cp "${GLUTEN_DIR}"/package/target/"${VELOX_BUNDLE_JAR}" gs://nqe-release-jars/native-dpgce-2.3/${OS_VERSION}/
fi
