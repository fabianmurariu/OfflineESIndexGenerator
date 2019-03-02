package com.github.fabianmurariu.esoffline

import java.net.URI

import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http._
import monix.execution.Scheduler
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class offlineTest extends FlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()


  "Offline Index" should "start an ES instance and configure the language ingest pipeline" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import offline._
    import spark.implicits._

    val a1: Dataset[WebDocument] = loadWETFiles("data/*.warc.wet.gz")
    val counts = a1.sample(0.1).indexPartitionHttp[Int](
      batchSize = 10,
      dest = new URI("./repo"), indices = Seq("docs1", "docs2"),
      init = EsLang.createPipeline) {
      (client: ElasticClient, wds: Seq[WebDocument]) =>
        client.execute(
          bulk(
            wds.map {
              wd =>
                indexInto("docs1" / "doc")
                  .fields("text" -> wd.text).routing("<CONSTANT>")
                  .refresh(RefreshPolicy.IMMEDIATE)
            }

          )
        ).await
        client.execute(
          bulk(
            wds.map {
              wd =>
                indexInto("docs2" / "doc")
                  .fields("text" -> wd.text).routing("<CONSTANT>")
                  .refresh(RefreshPolicy.IMMEDIATE)
            }

          )
        ).await
        1
    }.count

    assert(counts > 0)

    implicit val sched = Scheduler.io()
    implicit val fs:FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    EsLang.renameIndicesAndSnapshot("repo").runSyncUnsafe()

  }

  it should "rename indices and snapshots" ignore {
    implicit val sched = Scheduler.io()
    implicit val fs:FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    EsLang.renameSnapFilesInSegments("repo").runSyncUnsafe()

  }

}

