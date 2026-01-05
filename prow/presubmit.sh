#!/bin/bash
set -euxo pipefail

echo "Running as " "$(whoami)"
echo "Running in " "$(pwd)"

apt-get update && \
apt-get -y upgrade && \
apt-get install -y \
  wget \
  apt-transport-https \
  gpg \
  sudo \
  git \
  vim \
  curl \
  zip \
  unzip \
  tar \
  gcc \
  g++ \
  pkg-config \
  make \
  bison \
  autoconf \
  libtool \
  python3 \
  autoconf-archive \
  flex \
  ninja-build \
  python3-pip \
  ccache \
  gnupg \
  ca-certificates

# Installing gcloud cli
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
apt-get update && apt-get -y install google-cloud-cli

pip3 install cmake==3.28.3 --break-system-packages

cd /opt
wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg
echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list
apt-get update
apt-get install -y temurin-11-jdk
export JAVA_HOME="/usr/lib/jvm/temurin-11-jdk-amd64"

# Fetch Spark deps
SPARK_VERSION=3.5.3
SCALA_VERSION=2.12
GCS_LOCATION=$(gsutil ls gs://dataproc-performance/spark/${SPARK_VERSION}/scala-${SCALA_VERSION}/ | sort -V | tail -n 1)
GLUTEN_HOME="/home/prow/go/src/dataproc/third_party/apache/incubator-gluten"
SPARK_DEPS_DIR="${GLUTEN_HOME}/ep/build-spark/build/spark-deps"
mkdir -p "${SPARK_DEPS_DIR}/org" "${SPARK_DEPS_DIR}/com" "${SPARK_DEPS_DIR}/jars"
gsutil -m cp -r "${GCS_LOCATION}org/*" "${SPARK_DEPS_DIR}/org/"
gsutil -m cp -r "${GCS_LOCATION}com/*" "${SPARK_DEPS_DIR}/com/"

# Build NQE
cd /home/prow/go/src/dataproc/third_party/apache/incubator-gluten
mkdir -p .mvn && echo "-T1C" >> .mvn/maven.config
export MAVEN_OPTS="-Xss1g -Xmx20g -XX:MaxMetaspaceSize=10g -XX:ReservedCodeCacheSize=2g -Dsun.zip.disableMemoryMapping=true -DtrimStackTrace=false"
./dev/package.sh --build_tests=ON --velox_home=/home/prow/go/src/dataproc/third_party/oap-project/velox

# Copy pre-built Spark jars
mkdir -p "/home/prow/go/src/dataproc/third_party/apache/spark/assembly/target/scala-${SCALA_VERSION}/jars/"
gsutil -m cp -r "${GCS_LOCATION}jars/*" "/home/prow/go/src/dataproc/third_party/apache/spark/assembly/target/scala-${SCALA_VERSION}/jars/"

# Spark Integration tests
export SPARK_SCALA_VERSION=2.12
mvn clean install -Pjava-11 -Pscala-2.12 -Pspark-3.5.3 -Pspark-ut -Pbackends-velox -Pbigquery -Piceberg -Dbackend.home=/home/prow/go/src/dataproc/third_party/oap-project/velox -DtagsToExclude=org.apache.spark.tags.ExtendedSQLTest,org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.SkipTest -DargLine="-Dspark.test.home=/home/prow/go/src/dataproc/third_party/apache/spark -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true"
