#!/bin/bash

# Function to display usage information
usage() {
    echo "Usage: $0 <config_file>"
    exit 1
}

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Ensure a config file is provided
if [ $# -lt 1 ]; then
    usage
fi

# Source the config file
if [ -f "$1" ]; then
    source "$1"
else
    log "Config file not found: $1"
    exit 1
fi

# Check required parameters
required_params="environmentConfig masterURL persistLayer1"
for param in $required_params; do
    if [ -z "${!param}" ]; then
        log "Missing required parameter: $param"
        exit 1
    fi
done

# Construct Scala application parameters
scala_params="${environmentConfig} ${masterURL} ${persistLayer1}"

# Add optional parameters only if they are set and non-empty
[ -n "$runTimeProducts" ] && scala_params+=" ${runTimeProducts}"
[ -n "$runYear" ] && scala_params+=" ${runYear}"
[ -n "$runMonth" ] && scala_params+=" ${runMonth}"

# Your existing gcloud command with the Scala parameters appended
gcloud dataproc jobs submit spark --cluster=${dataprocClusterName} \
    --region=${region} --class="app.testingMain" \
    --jars=${mainJarFile},gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.31.1.jar,gs://spark-lib/spanner/spark-spanner-with-dependencies_2.12-0.40.0.jar \
    --labels="dataclassification=confidential" \
    --region=europe-west2 \
    --properties=spark.executor.memory=4g \
    --bucket=${tmp_bucket} -- ${scala_params}

exit_code=$?
if [ $exit_code -ne 0 ]; then
    log "Spark job failed with exit code $exit_code"
    exit $exit_code
else
    log "Spark job completed successfully"
fi
