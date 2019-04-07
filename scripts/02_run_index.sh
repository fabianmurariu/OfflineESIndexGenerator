#!/usr/bin/env bash

CLUSTER_ID=$(cat .cluster_id)

aws emr add-steps --cluster-id ${CLUSTER_ID} --steps Type=Spark,Name="IndexTheWorld",ActionOnFailure=CONTINUE,Args=[--class,com.github.fabianmurariu.esoffline.IndexTheWorld,s3://offline-elastic-world-index/deploy/OfflineESIndex-3.0.0.jar,--indices,CC-MAIN-2019-09,--hosts,ebay.be,--partitions,128,--snapshot-out,s3://offline-elastic-world-index/repo]