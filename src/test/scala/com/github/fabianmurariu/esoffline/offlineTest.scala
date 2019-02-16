package com.github.fabianmurariu.esoffline

import java.net.URI

import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class offlineTest extends FlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  "offline-indexing" should "trigger the offline indexing of a dataset" ignore {
    offline.loadWETFiles("data/*.warc.wet.gz").show(5)

  }

  it should "load the CC index and surface metadata" ignore {
    offline.loadIndexFiles("data/cdx-00000.gz").show(500, truncate = false)
  }

  it should "start an ES instance and configure the language ingest pipeline" in {
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

  }

}

