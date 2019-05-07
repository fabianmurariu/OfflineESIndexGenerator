#!/usr/bin/env bash

CLUSTER_ID=$(cat .cluster_id)

BUCKET=s3://offline-elastic-world-index
JAR=offline-index-cc-0.1-SNAPSHOT.jar
ELASTIC4S_VERSION=6.5.1
aws s3 rm --recursive ${BUCKET}/repo

aws emr add-steps --cluster-id ${CLUSTER_ID} --steps Type=Spark,Name="IndexTheWorld",ActionOnFailure=CONTINUE,Args=[--class,com.github.fabianmurariu.esoffline.IndexTheWorld,--executor-memory,12g,--packages,"com.sksamuel.elastic4s:elastic4s-core_2.11:${ELASTIC4S_VERSION}\,com.sksamuel.elastic4s:elastic4s-embedded_2.11:${ELASTIC4S_VERSION}\,com.sksamuel.elastic4s:elastic4s-circe_2.11:${ELASTIC4S_VERSION}\,com.sksamuel.elastic4s:elastic4s-http_2.11:${ELASTIC4S_VERSION}",--conf,spark.dynamicAllocation.enabled=true,--conf,spark.executor.extraJavaOptions=-Dlog4j2.debug,--conf,"spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/home/hadoop/extrajars/log4j2.xml",--driver-java-options=-Dlog4j.configurationFile=/home/hadoop/extrajars/log4j2.xml,${BUCKET}/deploy/${JAR},--indices,CC-MAIN-2019-09,--hosts,torontolawyers.ca,--store,true,--partitions,64,--snapshot-out,${BUCKET}/repo]
