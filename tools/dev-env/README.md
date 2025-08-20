# Development Environment

This document outlines the procedures for setting up and managing the development and build environments.

## Prerequisites

*   Install docker (go/installdocker)

*   Ensure the non-root user has access to docker (go/installdocker)

## Recommended Workflow

For an efficient development and testing process, we recommend the following setup. This separation helps keep your development work isolated from the build and testing process, preventing conflicts and ensuring clean builds.

1.  **Create two separate clones** of the Gluten repository.
    *   **Clone 1 (Development):** Use this for writing and debugging code.
    *   **Clone 2 (Release/Testing):** Use this for building artifacts to be tested on a cluster.
2.  **Set up the Development Environment:**
    *   In your first clone, run `./give-me-dev-env.sh`.
    *   This creates a persistent `dev` environment with IDE support, ideal for all development activities.
3.  **Set up the Release Environment:**
    *   In your second clone, run `./give-me-release-env.sh -g`.
    *   This creates a separate `release` environment. Use this to generate builds for cluster testing.
4.  **Production Builds:**
    *   For final, production-ready artifacts, use the `./give-me-jar.sh` script. This ensures a clean, isolated build from scratch.

## Environment Comparison

This table provides a high-level overview of the different environment types available.



|                    | dev                                                                  | release                                                                             | build                                                                               |
|--------------------|----------------------------------------------------------------------|-------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| IDE                | yes                                                                  | no                                                                                  | no                                                                                  |
| Persistent         | yes                                                                  | yes                                                                                 | no                                                                                  |
| Default Build Type | debug                                                                | release                                                                             | release                                                                             |
| Supported Build    | debug                                                                | debug <br>release                                                                   | debug <br>release                                                                   |
| OS-Java Support    | Deb12-Java11 <br>Deb12-Java17 (default)                              | Deb11-Java11 <br>Deb11-Java17 <br>Deb12-Java11 <br>Deb12-Java17 <br>Ubuntu22-Java11 | Deb11-Java11 <br>Deb11-Java17 <br>Deb12-Java11 <br>Deb12-Java17 <br>Ubuntu22-Java11 |
| Script             | give-me-dev-env.sh                                                   | give-me-release-env.sh                                                              | give-me-jar.sh                                                                      |
| Supported Scripts  | bash.sh <br>intellij.sh <br>clion.sh <br>start.sh <br>rebuild-env.sh | bash.sh -r <br>start.sh -r <br>rebuild-env.sh -r                                    |                                                                                     |

## Setting up an Environment

You can create a persistent environment for development or release, or perform a one-off build in a temporary environment.

### 1\. Development Environment (`dev`) - give-me-dev-env.sh

Creates a persistent development environment with IDE support. Recommended for active development.

| Supported Flags | Description                                                                        | Values                      |
|-----------------|------------------------------------------------------------------------------------|-----------------------------|
| -v              | Used to mount host directory to docker. Example -v ~/Developer:/root/dev/developer | (Optional)                  |
| -j              | Java Version                                                                       | Java11 <br>Java17 (Default) |

***Warning: All Velox changes, if any, will be overwritten. give-me-dev-env.sh should be used the first time the repository is cloned. Rerunning it will remove all Velox changes.***

After the setup script completes successfully, you can launch an IDE or a shell:

*   ./intellij.sh for IntelliJ

*   ./clion.sh for CLion

*   ./bash.sh for Bash

### 2\. Release Environment (`release`) - give-me-release-env.sh

Creates a persistent environment for building releases. It does not include an IDE setup.

| Supported Flags | Description                                                                        | Values                                       |
|-----------------|------------------------------------------------------------------------------------|----------------------------------------------|
| -v              | Used to mount host directory to docker. Example -v ~/Developer:/root/dev/developer | (Optional)                                   |
| -o              | OS Version                                                                         | Debian11 <br>Debian12 (Default) <br>Ubuntu22 |
| -j              | Java Version                                                                       | Java11 <br>Java17 (Default)                  |
| -d              | Debug Build Enabled                                                                | Release if not set                           |
| -g              | Fetches Velox repo from remote. **All Velox modification will be erased**          | (Optional)                                   |

### 3\. One-off Jar Build (`build`) - give-me-jar.sh

Creates a new, temporary build environment, builds the jar from scratch, and tears down the environment upon completion or failure. Ideal for clean, one-time builds.

| Supported Flags | Description                                                                        | Values                                       |
|-----------------|------------------------------------------------------------------------------------|----------------------------------------------|
| -v              | Used to mount host directory to docker. Example -v ~/Developer:/root/dev/developer | (Optional)                                   |
| -o              | OS Version                                                                         | Debian11 <br>Debian12 (Default) <br>Ubuntu22 |
| -j              | Java Version                                                                       | Java11 <br>Java17 (Default)                  |
| -d              | Debug Build Enabled                                                                | Release if not set                           |
| -g              | Fetches Velox repo from remote. **All Velox modification will be erased**          | (Optional)                                   |

## Interacting with the Environment

The following scripts are used to interact with the persistent dev and release environments.

### bash.sh `dev` `release`

Opens a bash shell inside the specified container.

| Supported Flags | Description          | Values                                       |
|-----------------|----------------------|----------------------------------------------|
| -o              | OS Version           | Debian11 <br>Debian12 (Default) <br>Ubuntu22 |
| -j              | Java Version         | Java11 <br>Java17 (Default)                  |
| -r              | Will use release env | By default will use dev env                  |

### intellij.sh & clion.sh `dev`

*   intellij.sh: Opens IntelliJ IDEA within the dev environment.

*   clion.sh: Opens CLion within the dev environment.


(These scripts do not take any flags and are only applicable to the dev environment.)

### start.sh `dev` `release`

Starts a container that has been stopped (e.g., after a system restart).

| Supported Flags | Description          | Values                                       |
|-----------------|----------------------|----------------------------------------------|
| -o              | OS Version           | Debian11 <br>Debian12 (Default) <br>Ubuntu22 |
| -j              | Java Version         | Java11<br>Java17 (Default)                   |
| -r              | Will use release env | By default will use dev env                  |

### rebuild-env.sh `dev` `release`

Quickly rebuilds the project inside an existing environment. It uses build caches for Velox and C++ Gluten and skips fetching dependencies to speed up the process. The Java build can be optionally skipped by modifying the `dev/package.sh` script.

| Supported Flags | Description            | Values                                       |
|-----------------|------------------------|----------------------------------------------|
| -o              | OS Version             | Debian11 <br>Debian12 (Default) <br>Ubuntu22 |
| -j              | Java Version           | Java11 <br>Java17 (Default)                  |
| -r              | Will use release env   | By default will use dev env                  |
| -s              | Rebuild Spark when set | By default this is not set                   |