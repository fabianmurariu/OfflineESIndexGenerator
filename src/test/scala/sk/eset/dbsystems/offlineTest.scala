package sk.eset.dbsystems

import com.github.fabianmurariu.esoffline.{WebDocument, offline}
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.http.HttpClient
import org.apache.http.HttpEntity
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

  it should "be able to join some of this stuff" in {
    import offline._
    import spark.implicits._
    import io.circe.generic.auto._
    import com.sksamuel.elastic4s.circe._
    import com.sksamuel.elastic4s.TcpClient
    import com.sksamuel.elastic4s.ElasticDsl._

    val a1: Dataset[WebDocument] = loadWETFiles("data/*.warc.wet.gz")
    def createPipeline(httpClient: HttpClient):HttpClient = {
      val restClient = httpClient.rest
      val body =
        """
          | {
          |  "description": "A pipeline to index data into language specific analyzers",
          |  "processors": [
          |    {
          |      "langdetect": {
          |        "field": "my_field",
          |        "target_field": "lang"
          |      }
          |    },
          |    {
          |      "script": {
          |        "source": "ctx.language = [:];ctx.language[ctx.lang] = ctx.remove('my_field')"
          |      }
          |    }
          |  ]
          | }
        """.stripMargin
      val params = new java.util.HashMap[String, String]()
      val stringEntity = new StringEntity(body)

      restClient.performRequest(
        "PUT",
        "_ingest/pipeline/langdetect-analyzer-pipeline",
        params,
        stringEntity, new BasicHeader("Accept", "application/json"), new BasicHeader("Content-Type", "application/json"))

      httpClient
    }

    a1.indexPartitionHttp[Int](5, createPipeline) {
      (client: HttpClient, wd: Seq[WebDocument]) =>
        1
    }

  }

}

