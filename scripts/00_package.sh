#!/usr/bin/env bash

BUCKET=s3://offline-elastic-world-index

rm -Rf offline_worker* && rm -Rf data/repo/* && sbt clean assembly
aws s3 cp offline-index-cc/target/scala-2.11/offline-index-cc-0.1-SNAPSHOT.jar ${BUCKET}/deploy/
aws s3 cp scripts/install_log4j2.sh ${BUCKET}/scripts/
