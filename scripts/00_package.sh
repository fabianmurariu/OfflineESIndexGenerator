#!/usr/bin/env bash

rm -Rf offline_worker* && rm -Rf data/repo/* && sbt clean assembly
aws s3 cp target/scala-2.11/OfflineESIndex-*jar s3://offline-elastic-world-index/deploy/
