#!/bin/bash

# Copyright 2019 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Install etcd
ETCD_VER=v3.4.9
DOWNLOAD_URL=https://storage.googleapis.com/etcd

curl -k -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
mkdir -p /var/opt/etcd
sudo tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /var/opt/etcd --strip-components=1
rm -f /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz

mkdir -p /var/run/etcd && chown -R vitess:vitess /var/run/etcd

# Clean up files we won't need in the final image.
rm -rf /var/lib/apt/lists/*
