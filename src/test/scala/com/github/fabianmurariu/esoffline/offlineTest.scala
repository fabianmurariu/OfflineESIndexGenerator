package com.github.fabianmurariu.esoffline

import java.net.URI

import com.optimaize.langdetect.i18n.LdLocale
import monix.execution.Scheduler
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class offlineTest extends FlatSpec with Matchers {

  "Offline Index" should "start an ES instance and configure the language ingest pipeline" ignore {
    implicit val spark: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    import offline._
    import spark.implicits._

    val a1: Dataset[WebDocument] = loadWETFiles("data/*.warc.wet.gz")
    val repo = new URI("./repo")

    val counts = a1.sample(0.1).indexPartitionHttp2[Int, String => Option[LdLocale]](10, repo).count

    assert(counts > 0)

    implicit val scheduler = Scheduler.io()
    implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    EsLang.renameIndicesAndSnapshot("repo").runSyncUnsafe()

  }

}

