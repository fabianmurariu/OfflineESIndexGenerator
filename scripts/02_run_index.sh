#!/usr/bin/env bash

CLUSTER_ID=$(cat .cluster_id)

BUCKET=s3://offline-elastic-world-index

aws emr add-steps --cluster-id ${CLUSTER_ID} --steps Type=Spark,Name="IndexTheWorld",ActionOnFailure=CONTINUE,Args=[--class,com.github.fabianmurariu.esoffline.IndexTheWorld,--executor-memory,12g,--packages,"com.sksamuel.elastic4s:elastic4s-core_2.11:6.5.1\,com.sksamuel.elastic4s:elastic4s-embedded_2.11:6.5.1\,com.sksamuel.elastic4s:elastic4s-circe_2.11:6.5.1\,com.sksamuel.elastic4s:elastic4s-http_2.11:6.5.1",--conf,spark.dynamicAllocation.enabled=true,--conf,spark.executor.extraJavaOptions=-Dlog4j2.debug,--conf,"spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/home/hadoop/extrajars/log4j2.xml",--driver-java-options=-Dlog4j.configurationFile=/home/hadoop/extrajars/log4j2.xml,s3://${BUCKET}/deploy/OfflineESIndex-3.0.0.jar,--indices,CC-MAIN-2019-09,--hosts,torontolawyers.ca,--partitions,64,--snapshot-out,s3://${BUCKET}/repo]
