#!/bin/bash
# Copyright VMware, Inc.
# SPDX-License-Identifier: APACHE-2.0

# shellcheck disable=SC1091

set -o errexit
set -o nounset
set -o pipefail
#set -o xtrace

# Load libraries
. /opt/bitnami/scripts/libspark.sh
. /opt/bitnami/scripts/libos.sh

# Load Spark environment settings
. /opt/bitnami/scripts/spark-env.sh

if [ "$SPARK_MODE" == "master" ]; then
    # Master constants
    EXEC=$(command -v start-master.sh)
    ARGS=()
    EXEC_CONNECT=$(command -v start-connect-server.sh) 
    info "** Starting Spark in master mode **"
else
    # Worker constants
    EXEC=$(command -v start-worker.sh)
    ARGS=("$SPARK_MASTER_URL")
    info "** Starting Spark in worker mode **"
fi
if am_i_root; then
    # exec_as_user 
    if [ "$SPARK_MODE" == "master" ]; then
        "$SPARK_DAEMON_USER" "$EXEC" "${ARGS[@]-}" &
        "$SPARK_DAEMON_USER" "$EXEC_CONNECT" --packages org.apache.spark:spark-connect_2.12:3.5.1
    else
        exec "$SPARK_DAEMON_USER" "$EXEC" "${ARGS[@]-}"
    fi
else
    # exec 
    if [ "$SPARK_MODE" == "master" ]; then
        "$EXEC" "${ARGS[@]-}" &
        "$EXEC_CONNECT" --packages org.apache.spark:spark-connect_2.12:3.5.1
    else
        exec "$EXEC" "${ARGS[@]-}"
    fi
fi
