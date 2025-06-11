#!/bin/bash
set -euxo pipefail

echo "Running as " "$(whoami)"
echo "Running in " "$(pwd)"

# Install pre-requisites
apt-get update && apt-get install -y ca-certificates \
  apt-transport-https gnupg2 software-properties-common \
  lsb-release curl wget sudo && \
  update-ca-certificates
apt-get update

# tzdata prompts for a timezone during apt-install, tzdata is used in implementing datetime
export TZ=Asia/Kolkata
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Add the Docker apt-repository
curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg \
    | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
apt-get update -y && \
    apt-get install -yqq --no-install-recommends docker-ce && \
    rm -rf /var/lib/apt/lists/* && \
    sed -i 's/cgroupfs_mount$/#cgroupfs_mount\n/' /etc/init.d/docker \
    && update-alternatives --set iptables /usr/sbin/iptables-legacy \
    && update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy

# Move Docker's storage location
echo 'DOCKER_OPTS="${DOCKER_OPTS} --data-root=/docker-graph"' | \
    tee --append /etc/default/docker

# Check if job has opted-in to docker-in-docker availability.
export DOCKER_IN_DOCKER_ENABLED=${DOCKER_IN_DOCKER_ENABLED:-true}
if [[ "${DOCKER_IN_DOCKER_ENABLED}" == "true" ]]; then
    echo "Docker in Docker enabled, initializing..."
    printf '=%.0s' {1..80}; echo
    # If we have opted in to docker in docker, start the docker daemon,
    sed -i 's/^.*ulimit -Hn 524288.*$/#&/' /etc/init.d/docker
    sed -i '/if \[ "$BASH" \]; then/,/fi/ s/^/#/' /etc/init.d/docker
    service docker start
    # the service can be started but the docker socket not ready, wait for ready
    WAIT_N=0
    MAX_WAIT=5
    while true; do
        # docker ps -q should only work if the daemon is ready
        docker ps -q > /dev/null 2>&1 && break
        if [[ ${WAIT_N} -lt ${MAX_WAIT} ]]; then
            WAIT_N=$((WAIT_N+1))
            echo "Waiting for docker to be ready, sleeping for ${WAIT_N} seconds."
            sleep ${WAIT_N}
        else
            echo "Reached maximum attempts, not waiting any longer..."
            break
        fi
    done
    printf '=%.0s' {1..80}; echo
    echo "Done setting up docker in docker."

    # Workaround for https://github.com/kubernetes/test-infra/issues/23741
    # Instead of removing, disabled by default in case we need to address again
    if [[ "${BOOTSTRAP_MTU_WORKAROUND:-"false"}" == "true" ]]; then
        echo "configure iptables to set MTU"
        iptables -t mangle -A POSTROUTING -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu
    fi
fi

# Download pre-packaged Spark
cd /opt && \
wget -nv https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz && \
tar --strip-components=1 -xf spark-3.5.3-bin-hadoop3.tgz spark-3.5.3-bin-hadoop3/jars/ && \
rm -rf spark-3.5.3-bin-hadoop3.tgz && \
mkdir -p /opt/shims/spark35/spark_home/assembly/target/scala-2.12 && \
mv jars /opt/shims/spark35/spark_home/assembly/target/scala-2.12 && \
wget -nv https://github.com/apache/spark/archive/refs/tags/v3.5.3.tar.gz && \
tar --strip-components=1 -xf v3.5.3.tar.gz spark-3.5.3/sql/core/src/test/resources/  && \
mkdir -p shims/spark35/spark_home/ && \
mv sql shims/spark35/spark_home/

# Build NQE
cd /home/prow/go/src/dataproc/third_party/apache/incubator-gluten
./tools/dev-env/give-me-release-env.sh -v /home/prow/go/src/dataproc/third_party/oap-project/velox:/root/incubator-gluten/ep/build-velox/build/velox_ep/ \
  -v /opt/shims/spark35/spark_home/:/opt/shims/spark35/spark_home

CONTAINER_NAME=$(cat "tools/dev-env/.container/release-env-Debian12-Java17")

# Spark starts pyspark env for some tests
docker exec -i "${CONTAINER_NAME}" bash -c 'pip3 install setuptools pyspark==3.5.3 cython pandas pyarrow --break-system-packages'

# Spark Integration tests
docker exec -i "${CONTAINER_NAME}" bash -c 'pwd && export SPARK_SCALA_VERSION=2.12 && \
  mvn clean install -Pjava-17 -Pscala-2.12 -Pspark-3.5 -Pspark-ut -Pbackends-velox \
  -Dbackend.home=/home/prow/go/src/dataproc/third_party/oap-project/velox \
  -DtagsToExclude=org.apache.spark.tags.ExtendedSQLTest,org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.SkipTestTags \
  -DargLine="-Dspark.test.home=/opt/shims/spark35/spark_home -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true"'

# Remove docker container
docker rm -f "${CONTAINER_NAME}"
