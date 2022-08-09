#!/usr/bin/env bash

PYINSTALLER_VERSION=5.3

docker build --build-arg "PYINSTALLER_VERSION=${PYINSTALLER_VERSION}" -t pyinstaller:${PYINSTALLER_VERSION} -f ./Dockerfile.amd64 .