FROM debian:11

# Set a default timezone, can be overriden via ARG
ARG tz="Europe/Madrid"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# TZ and DEBIAN_FRONTEND="noninteractive"
# are required to avoid tzdata installation
# to prompt for region selection.
ENV DEBIAN_FRONTEND="noninteractive" TZ=${tz}

RUN apt-get update && \
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

RUN pip3 install cmake==3.28.3

RUN /usr/sbin/update-ccache-symlinks
ENV PATH="/usr/lib/ccache:${PATH}"

WORKDIR /opt

RUN wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg
RUN echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list
RUN apt-get update
RUN apt-get install -y temurin-17-jdk
ENV JAVA_HOME="/usr/lib/jvm/temurin-17-jdk-amd64"

#MAVEN
ARG MAVEN_VERSION="3.9.10"
WORKDIR /opt
RUN wget https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz
RUN tar xzf apache-maven-${MAVEN_VERSION}-bin.tar.gz
RUN rm apache-maven-${MAVEN_VERSION}-bin.tar.gz
ENV M2_HOME=/opt/apache-maven-${MAVEN_VERSION}
RUN update-alternatives --install /usr/bin/mvn mvn /opt/apache-maven-${MAVEN_VERSION}/bin/mvn 900

#PYTHON
ENV PIP_BREAK_SYSTEM_PACKAGES=1
RUN python3 -m pip install --upgrade pip

WORKDIR /root/incubator-gluten

# Prevent container from exiting
CMD ["tail", "-f", "/dev/null"]
