#!/bin/bash

export BASE_DIR="/tmpfs/src/git/"
export GLUTEN_DIR="${BASE_DIR}/incubator-gluten"

function download_maven() {
  MAVEN_VERSION=3.9.10
  wget https://archive.apache.org/dist/maven/maven-3/"${MAVEN_VERSION}"/binaries/apache-maven-"${MAVEN_VERSION}"-bin.tar.gz
  tar -xvzf apache-maven-"${MAVEN_VERSION}"-bin.tar.gz apache-maven-"${MAVEN_VERSION}"
  export PATH=/tmpfs/src/apache-maven-"${MAVEN_VERSION}"/bin:$PATH
}

function build_jars() {
  OS_VERSION=$1
  JAVA_VERSION=$2
  SPARK_VERSION=$3
  SCALA_VERSION=$4

  chmod 755 "${GLUTEN_DIR}"/tools/dev-env/give-me-jar.sh
  /bin/bash "${GLUTEN_DIR}"/tools/dev-env/give-me-jar.sh -v "${BASE_DIR}"/velox/:/root/incubator-gluten/ep/build-velox/build/velox_ep/ \
    -o "${OS_VERSION}" -j "${JAVA_VERSION}" -s "${SPARK_VERSION}" -c "${SCALA_VERSION}"
}

function upload_jar() {
    ARTIFACT_ID=$1
    VERSION=$2
    DESCRIPTION=$3
    JAR_PATH=$4
    mvn deploy:deploy-file -DgroupId=org.apache.gluten -DartifactId="${ARTIFACT_ID}" -Dversion="${VERSION}" -DgeneratePom=true -DgeneratePom.description="${DESCRIPTION}" -Dpackaging=jar -Dfile="${JAR_PATH}" -DrepositoryId=artifact-registry -Durl=artifactregistry://us-central1-maven.pkg.dev/cloud-dataproc-prod/dataproc-internal -U -X
}
