#!/bin/bash
set -exu

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

function fetch_spark_deps() {
  local SPARK_VERSION=$1
  local SCALA_VERSION=$2
  local GCS_LOCATION=$(gsutil ls gs://dataproc-performance/spark/${SPARK_VERSION}/scala-${SCALA_VERSION}/ \
  | sort -V | tail -n 1)
  mkdir -p ${CURRENT_DIR}/../build/spark-deps

  gsutil -m cp -r "${GCS_LOCATION}*" ${CURRENT_DIR}/../build/spark-deps/
}

function copy_spark_deps_to_local() {
  mkdir -p ${HOME}/.m2/repository
  cp -r ${CURRENT_DIR}/../build/spark-deps/* ${HOME}/.m2/repository/
}
