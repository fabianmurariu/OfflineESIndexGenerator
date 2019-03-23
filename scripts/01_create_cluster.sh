#!/usr/bin/env bash

set -e

CLUSTER_NAME=${1?"Cluster Name required"}

CLUSTERID=$(aws emr create-cluster \
                --name "$CLUSTER_NAME" \
                 --tags "Name=$CLUSTER_NAME" \
                 --release-label emr-5.21.0 \
                 --applications Name=Ganglia Name=Hadoop Name=Spark \
                 --use-default-roles \
                 --auto-terminate \
                 --enable-debugging \
                 --log-uri 's3n://aws-logs-bytes32-us-east-1/elasticmapreduce/' \
                 --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master instance group","BidPrice":"0.20"},{"InstanceCount":4,"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core instance group","BidPrice":"0.20"}]' \
                 --visible-to-all-users | grep -o 'j-\w*')
echo ${CLUSTERID}