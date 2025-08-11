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
ccache

pip3 install cmake==3.28.3 --break-system-packages

cd /opt
wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg
echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list
apt-get update
apt-get install -y temurin-11-jdk
export JAVA_HOME="/usr/lib/jvm/temurin-11-jdk-amd64"

# Build suffle-support
cd /home/prow/go/src/dataproc/shuffle-support
./gradlew publishToMavenLocal

# Build spark
echo "Building Spark..."
cd /home/prow/go/src/dataproc/third_party/apache/spark
mkdir -p .mvn && echo "-T1C" >> .mvn/maven.config
export MAVEN_OPTS="-Xss1g -Xmx20g -XX:MaxMetaspaceSize=10g -XX:ReservedCodeCacheSize=2g -Dsun.zip.disableMemoryMapping=true -DtrimStackTrace=false"
./build/mvn -DskipTests -Pdataproc-ip-protect-dev clean install

# Build NQE
cd /home/prow/go/src/dataproc/third_party/apache/incubator-gluten
mkdir -p .mvn && echo "-T1C" >> .mvn/maven.config
export MAVEN_OPTS="-Xss1g -Xmx20g -XX:MaxMetaspaceSize=10g -XX:ReservedCodeCacheSize=2g -Dsun.zip.disableMemoryMapping=true -DtrimStackTrace=false"
./dev/package.sh --velox_home=/home/prow/go/src/dataproc/third_party/oap-project/velox

# Spark Integration tests
export SPARK_SCALA_VERSION=2.12
mvn clean install -Pjava-11 -Pscala-2.12 -Pspark-3.5.3 -Pspark-ut -Pbackends-velox -Pbigquery -Dbackend.home=/home/prow/go/src/dataproc/third_party/oap-project/velox -DtagsToExclude=org.apache.spark.tags.ExtendedSQLTest,org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.SkipTestTags -DargLine="-Dspark.test.home=/home/prow/go/src/dataproc/third_party/apache/spark -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true"
