#!/bin/bash
set -exu

SPARK_VERSION=3.5.3
SCALA_VERSION=2.12
SPARK_HOME=""

OS=`uname -s`
ARCH=`uname -m`

for arg in "$@"; do
  case $arg in
  --scala_version=*)
    SCALA_VERSION=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --spark_version=*)
    SPARK_VERSION=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
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

# Copy spark dependencies to local maven repo
mkdir -p /root/.m2/repository/
cp -r $SPARK_HOME/../spark-deps/* /root/.m2/repository/

cd $SPARK_HOME
git checkout dataproc-branch-${SPARK_VERSION}

# Build Spark
echo "Building Spark..."
mkdir -p .mvn
cat <<EOF > .mvn/maven.config
-T1C
EOF
export MAVEN_OPTS="-Xss1g -Xmx20g -XX:MaxMetaspaceSize=10g -XX:ReservedCodeCacheSize=2g -Dsun.zip.disableMemoryMapping=true -DtrimStackTrace=false"

./dev/change-scala-version.sh "${SCALA_VERSION}"

./build/mvn -DskipTests clean install -Pdataproc-ip-protect-dev -Pscala-${SCALA_VERSION}
echo "Successfully built Spark from source."
