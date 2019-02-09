package sk.eset.dbsystems

import com.github.fabianmurariu.esoffline.{EsLang, WebDocument, offline}
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http._
import monix.eval.Task
import monix.execution.Callback
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class offlineTest extends FlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

  "offline-indexing" should "trigger the offline indexing of a dataset" in {
    offline.loadWETFiles("data/*.warc.wet.gz").show(5)

  }

  it should "load the CC index and surface metadata" in {
    offline.loadIndexFiles("data/cdx-00000.gz").show(500, truncate = false)
  }

  it should "start an ES instance and configure the language ingest pipeline" in {
    import offline._
    import spark.implicits._
    import io.circe.generic.auto._
    import com.sksamuel.elastic4s.circe._
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val a1: Dataset[WebDocument] = loadWETFiles("data/*.warc.wet.gz")
    val counts = a1.indexPartitionHttp[Int](5, EsLang.createPipeline) {
      (client: ElasticClient, wds: Seq[WebDocument]) =>
        client.execute(
          bulk(
            wds.map{
              wd =>
                indexInto("docs" / "doc")
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

