#!/bin/bash

set -ex

#IDEA AND CLION
apt update && apt install -y libcanberra-gtk3-module

#CLION
CLION_VERSION=2025.1.2
CLION_BUILD=2025.1.2
clion_source=https://download.jetbrains.com/cpp/CLion-${CLION_BUILD}.tar.gz
clion_local_dir=.CLion${CLION_BUILD}
mkdir -p /opt/clion
cd /opt/clion
wget -O /opt/clion/installer.tgz $clion_source && \
  tar --strip-components=1 -xzf installer.tgz && \
  rm installer.tgz
mkdir /root/.CLion && ln -sf /root/.CLion /root/$clion_local_dir

#INTELIJ
IDEA_VERSION=2025.1.2
IDEA_BUILD=2025.1.2
idea_source=https://download.jetbrains.com/idea/ideaIC-${IDEA_BUILD}.tar.gz
idea_local_dir=.IdeaIC${IDEA_VERSION}
mkdir -p /opt/idea
cd /opt/idea
wget -O /opt/idea/installer.tgz $idea_source && \
  tar --strip-components=1 -xzf installer.tgz && \
  rm installer.tgz
mkdir /root/.Idea && ln -sf /root/.Idea /root/$idea_local_dir

cd /root/incubator-gluten
git config --global --add safe.directory "*"
