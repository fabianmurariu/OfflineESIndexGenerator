package com.github.fabianmurariu.esoffline

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class offlineTest extends FlatSpec with Matchers {

  "Offline Index" should "start an ES instance and configure the language ingest pipeline" ignore  {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    import spark.implicits._
    spark.read.parquet("data/index-demo").groupBy("url_host_private_domain").count().orderBy('count.desc).show(1000)
  }

}

