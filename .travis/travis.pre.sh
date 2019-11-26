#!/bin/bash

set -exuo pipefail # Strict shell

if [ "${TNT_VERSION}" = "np/gh-2592-prepared-statemets-v2" ]; then
    sudo apt-get update

    # branch: np/gh-2592-prepared-statemets-v2
    # commit: ae06bd6e176c20b065f2a503461f7e907b32a96f
    wget https://tkn.me/tmp/tarantool_2.3.0.266.gae06bd6-1_amd64.deb
    wget https://tkn.me/tmp/tarantool-common_2.3.0.266.gae06bd6-1_all.deb

    # Install dependencies and tarantool packages.
    sudo apt-get -y install binutils libgomp1 libicu52 libunwind8
    sudo dpkg -i tarantool*.deb
    sudo apt-get -y install -f  # just in case

    sudo tarantoolctl stop example

    exit
fi

curl http://download.tarantool.org/tarantool/${TNT_VERSION}/gpgkey | sudo apt-key add -
RELEASE=`lsb_release -c -s`

sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
sudo tee /etc/apt/sources.list.d/tarantool_${TNT_VERSION/./_}.list <<- EOF
deb http://download.tarantool.org/tarantool/${TNT_VERSION}/ubuntu/ ${RELEASE} main
deb-src http://download.tarantool.org/tarantool/${TNT_VERSION}/ubuntu/ ${RELEASE} main
EOF

sudo apt-get update
sudo apt-get -y install tarantool tarantool-common

sudo tarantoolctl stop example
