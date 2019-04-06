#!/usr/bin/env bash

set -e

CLUSTER_NAME="OfflineIndexer"

if [[ ! -f .cluster_id ]]; then
CLUSTER_ID=$(aws emr create-cluster \
                --name "$CLUSTER_NAME" \
                 --tags "Name=$CLUSTER_NAME" \
                 --release-label emr-5.21.0 \
                 --applications Name=Ganglia Name=Hadoop Name=Spark \
                 --use-default-roles \
                 --ec2-attributes SubnetId=subnet-0f14fbb22e0f0d7e0,KeyName=OfflineIndexer-Emr \
                 --enable-debugging \
                 --log-uri 's3n://aws-logs-bytes32-us-east-1/elasticmapreduce/' \
                 --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master instance group"},{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"r5.xlarge","Name":"Core instance group","BidPrice":"0.20"}]' \
                 --visible-to-all-users | grep -o 'j-\w*')
echo ${CLUSTER_ID}
echo ${CLUSTER_ID} > .cluster_id
fi