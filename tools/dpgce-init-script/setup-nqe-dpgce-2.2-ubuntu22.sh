#!/bin/bash

set -ex

gsutil cp gs://dataproc-experimental/gluten-velox-bundle-spark3.5_2.12-ubuntu_22.04_x86_64-1.2.1-SNAPSHOT.jar /usr/lib/spark/jars/gluten-velox-bundle-spark3.5_2.12-ubuntu_22.04_x86_64-1.2.1-SNAPSHOT.jar
gsutil cp gs://dataproc-experimental/gluten-thirdparty-lib-ubuntu-22.04-x86_64.jar /usr/lib/spark/jars/gluten-thirdparty-lib-ubuntu-22.04-x86_64.jar
gsutil cp gs://dataproc-experimental/jemalloc-5.3.0.tar.bz2 /opt/jemalloc-5.3.0.tar.bz2
tar -xvjf /opt/jemalloc-5.3.0.tar.bz2 -C /opt
rm /opt/jemalloc-5.3.0.tar.bz2
cd /opt/jemalloc-5.3.0 && sudo ./configure && sudo make && sudo make install
