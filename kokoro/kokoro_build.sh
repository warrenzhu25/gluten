#!/bin/bash
set -ex

PATH_TO_CURRENT_DIR=$(dirname $0)
source "${PATH_TO_CURRENT_DIR}"/jar_util.sh

echo "Running as " "$(whoami)"

download_maven

LATEST_GLUTEN_COMMIT_ID="${KOKORO_GOB_COMMIT_incubator_gluten}"
GLUTEN_METADATA_KEY_VALUE="gluten-commit-id:"$LATEST_GLUTEN_COMMIT_ID

LATEST_VELOX_COMMIT_ID="${KOKORO_GOB_COMMIT_velox}"
VELOX_METADATA_KEY_VALUE="velox-commit-id:"$LATEST_VELOX_COMMIT_ID

build_jars "${OS_VERSION}" "${JAVA_VERSION}" "${SPARK_VERSION}" "${SCALA_VERSION}"

TODAY=$(date +%Y%m%d-%H%M)
VERSION="1.4.0-${TODAY}"
DESCRIPTION="Gluten 1.4 with Spark ${SPARK_VERSION} jars for ${IMAGE_TYPE}-${IMAGE_VERSION}"
VELOX_BUNDLE_JAR="gluten-velox-bundle-spark${SPARK_VERSION}_${SCALA_VERSION}-linux_amd64-1.4.0.jar"

# Upload to Dataproc Artifact Registry
if [ "$PUBLISH_JARS_TO_AR" == "yes" ]; then
  cp "${GLUTEN_DIR}"/package/target/"${VELOX_BUNDLE_JAR}" "${BASE_DIR}"

  cd "${GLUTEN_DIR}"
  upload_jar "${IMAGE_TYPE}-${IMAGE_VERSION}-velox-bundle-${OS_VERSION}" "${VERSION}" "${DESCRIPTION}" "${BASE_DIR}/${VELOX_BUNDLE_JAR}"
fi

# Upload to GCS Location
if [ "$PUBLISH_JARS_TO_GCS" == "yes" ]; then
  gsutil cp "${GLUTEN_DIR}"/package/target/"${VELOX_BUNDLE_JAR}" gs://nqe-release-jars/native-${IMAGE_TYPE}-${IMAGE_VERSION}/${OS_VERSION}/
  gsutil setmeta -h "x-goog-meta-"$GLUTEN_METADATA_KEY_VALUE gs://nqe-release-jars/native-${IMAGE_TYPE}-${IMAGE_VERSION}/${OS_VERSION}/*
  gsutil setmeta -h "x-goog-meta-"$VELOX_METADATA_KEY_VALUE gs://nqe-release-jars/native-${IMAGE_TYPE}-${IMAGE_VERSION}/${OS_VERSION}/*
fi

sudo rm -rf "${GLUTEN_DIR}"
