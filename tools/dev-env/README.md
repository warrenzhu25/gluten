# Development Environment

This document outlines the procedures for setting up and managing the development and build environments.

## Prerequisites

*   Install docker (go/installdocker)

*   Ensure the non-root user has access to docker (go/installdocker)

*   Ensure java version is set to 11

*   Ensure gcloud cli is installed (go/installdocker#gcloud-recommended)

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

## Instructions to setup CLion for Velox
1. Run below commands from inside the Velox home directory
```
git apply prow/velox-prow-build.patch
PROMPT_ALWAYS_RESPOND=Y ./scripts/setup-ubuntu.sh
PROMPT_ALWAYS_RESPOND=Y ./scripts/setup-adapters.sh gcs
```
2. Make sure `Cmake` version is set to `3.28.3` in `Settings -> Build, Execution, Deployment -> Toolchains`
3. Also, add these configs `-DVELOX_ENABLE_PARQUET=ON -DVELOX_BUILD_TESTING=ON -DVELOX_ENABLE_GCS=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=/path/to/deps-download-folder-in-velox` to CMake options and click Apply.


## FAQ
#### 1. The Docker container is stopped (but still existing). I want to re-run my Dev environment and re-launch IDEs.
```
./start.sh
```
#### 2. The Docker container is removed. I want to re-run my Dev environment and re-launch IDEs without reruning the entire ./give-me-dev-env.sh.
Note: you can follow the same steps if you are changing to a new Cloudtop, or you want to re-use the Dev environment shared by other persons.

Step 1: Get the Docker image ID and rerun it.
```
# get the docker image ID, e.g. 89369a174d99
docker image ls

# go to incubator-gluten project root directory

# run the docker image, and give it an arbitary container name, e.g. gluten-dev
docker run -d --name gluten-dev \
  -e DISPLAY=$DISPLAY \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -v "$(pwd)":/opt/gluten \
  --net=host \
  89369a174d99
```

If the container name, e.g. gluten-dev is being used, and you really want to remove it, run
```
docker rm -f gluten-dev 2>/dev/null
```

[Troubleshooting only] Step 2: make sure your xhost is set up correctly
```
xhost +local:
# you should have the output below
# non-network local connections being added to access control list
```

Step 3: launch the IDEs, e.g. IntelliJ
```
docker exec -d \
  -e DISPLAY=$DISPLAY \
  -e QT_X11_NO_MITSHM=1 \
  -e _JAVA_AWT_WM_NONREPARENTING=1 \
  gluten-dev /opt/idea/bin/idea.sh
```
When IntelliJ opens, select Open and navigate to /opt/gluten (the internal mount point) to load the project.
