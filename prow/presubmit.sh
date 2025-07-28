#!/bin/bash
set -euxo pipefail

echo "Running as " "$(whoami)"
echo "Running in " "$(pwd)"

echo "Checking whether pre-packaged Spark is installed correctly by printing Spark dir."
ls /opt/shims/spark35/spark_home

# Build NQE
cd /home/prow/go/src/dataproc/third_party/apache/incubator-gluten
mkdir -p .mvn && echo "-T1C" >> .mvn/maven.config
export MAVEN_OPTS="-Xss1g -Xmx20g -XX:MaxMetaspaceSize=10g -XX:ReservedCodeCacheSize=2g -Dsun.zip.disableMemoryMapping=true -DtrimStackTrace=false"
./dev/package.sh --velox_home=/home/prow/go/src/dataproc/third_party/oap-project/velox

# Spark Integration tests
export SPARK_SCALA_VERSION=2.12
mvn clean install -Pjava-17 -Pscala-2.12 -Pspark-3.5.3 -Pspark-ut -Pbackends-velox -Pbigquery -Dbackend.home=/home/prow/go/src/dataproc/third_party/oap-project/velox -DtagsToExclude=org.apache.spark.tags.ExtendedSQLTest,org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.SkipTestTags -DargLine="-Dspark.test.home=/opt/shims/spark35/spark_home -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true"
