#!/bin/bash

set -eux

export PATH="$GITHUB_WORKSPACE/python3-venv/bin:$PATH"

pip3 install 'moto[s3]' flask flask_cors boto3
