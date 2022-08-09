#!/usr/bin/env bash

PYINSTALLER_VERSION=5.3

docker run -v "$(pwd):/opt/src/" pyinstaller:${PYINSTALLER_VERSION} --clean -y --onefile ./kafka-connect.py
