#!/bin/bash
set -exu

SHUFFLE_SUPPORT_REPO=sso://bigdataoss-internal/shuffle-support
SPARK_REPO=sso://bigdataoss-internal/third_party/apache/spark
SPARK_HOME=""

OS=`uname -s`

for arg in "$@"; do
  case $arg in
  --spark_home=*)
    SPARK_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

if [ "$SPARK_HOME" == "" ]; then
  SPARK_HOME="$CURRENT_DIR/../build/spark"
fi
SHUFFLE_SUPPORT_HOME="$SPARK_HOME/../shuffle-support"

function checkout_spark {
  echo "Fetching Spark source code..."
  git clone ${SPARK_REPO} ${SPARK_HOME}
  echo "Finished getting Spark code"
}

function checkout_and_build_shuffle_support {
  echo "Fetching Shuffle Support source code..."
  git clone ${SHUFFLE_SUPPORT_REPO} ${SHUFFLE_SUPPORT_HOME}
  echo "Finished getting Shuffle Support code."

  echo "Building Shuffle support..."
  cd ${SHUFFLE_SUPPORT_HOME}
  ./gradlew publishToMavenLocal

  mkdir -p ${SPARK_HOME}/../spark-deps/com/google/cloud/bigdataoss/
  cp -r ~/.m2/repository/com/google/cloud/bigdataoss/shuffle-endpoints ${SPARK_HOME}/../spark-deps/com/google/cloud/bigdataoss/
  echo "Finished building Shuffle support."
}

function download_spark_deps() {
  echo "Starting download of Spark deps..."
  cd $SPARK_HOME
  mvn dependency:resolve -Dmaven.repo.local=${SPARK_HOME}/../spark-deps
  echo "Finished download of all Spark deps."
}

checkout_and_build_shuffle_support
checkout_spark
download_spark_deps
