# Development Environment

## Prerequisites
- Install `docker`
- Non-root user have access to `docker`

## Build only jar
- To build jar with release `./give-me-jar.sh`. This uses Debian 12 and Java 17 by default.
- To build jar with debug `./give-me-jar.sh -d`
- To use different build OS `./give-me-jar.sh -o Debian12`. Currently only `Debian12` is supported.
- To use different Java version `./give-me-jar.sh -j Java11`. Currently only `Java11` and `Java17` is supported.

## Create Dev Env
- Run `./give-me-dev-env.sh`. This sets up a dev environment with IDE.
- After this completes successfully run
  - `./intellij.sh` for IntelliJ
  - `./clion.sh` for CLion
  - `./bash.sh` for Bash
- Inside container environment (Either through bash or containerized IDE's terminal) the following scripts are available:
  - `./root/incubator-gluten/tools/dev-env/docker-scripts/package.sh --build_type=Debug` for Debug Build. Recommended for development.
  - `./root/incubator-gluten/tools/dev-env/docker-scripts/package.sh` for Release Build.
- If system restarted or container has stopped, it can be started using `./start.sh`.

Dev Env uses Debian 12 and Java 17 only.