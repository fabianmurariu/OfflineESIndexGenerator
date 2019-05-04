#!/usr/bin/env bash

BUCKET=s3://offline-elastic-world-index

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
                 --bootstrap-action Path=${BUCKET}/scripts/install_log4j2.sh \
                 --configurations file://./scripts/spark-defaults-override.json \
                 --log-uri ${BUCKET}/logs \
                 --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"r5.2xlarge","Name":"Master instance group","BidPrice":"0.35"}]' \
                 --visible-to-all-users | grep -o 'j-\w*')

echo ${CLUSTER_ID}
echo ${CLUSTER_ID} > .cluster_id
fi
