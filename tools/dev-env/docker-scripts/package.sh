#!/bin/bash

set -ex

export PROMPT_ALWAYS_RESPOND=n
/root/incubator-gluten/dev/package.sh --enable_ep_cache=ON --build_arrow=OFF "$@"
