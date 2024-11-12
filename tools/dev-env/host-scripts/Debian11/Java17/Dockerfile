FROM debian:11

# Set a default timezone, can be overriden via ARG
ARG tz="Europe/Madrid"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# TZ and DEBIAN_FRONTEND="noninteractive"
# are required to avoid tzdata installation
# to prompt for region selection.
ENV DEBIAN_FRONTEND="noninteractive" TZ=${tz}

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    wget \
    cmake \
    ninja-build \
    sudo \
    vim \
    apt-transport-https \
    xclip \
    libre2-dev \
    python3-pip \
    openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

#MAVEN
WORKDIR /opt
RUN wget https://dlcdn.apache.org/maven/maven-3/3.9.5/binaries/apache-maven-3.9.5-bin.tar.gz
RUN tar xzf apache-maven-3.9.5-bin.tar.gz
RUN rm apache-maven-3.9.5-bin.tar.gz
ENV M2_HOME=/opt/apache-maven-3.9.5
RUN update-alternatives --install /usr/bin/mvn mvn /opt/apache-maven-3.9.5/bin/mvn 900

#PYTHON
ENV PIP_BREAK_SYSTEM_PACKAGES=1
RUN python3 -m pip install --upgrade pip

WORKDIR /root/incubator-gluten

# Prevent container from exiting
CMD ["tail", "-f", "/dev/null"]
