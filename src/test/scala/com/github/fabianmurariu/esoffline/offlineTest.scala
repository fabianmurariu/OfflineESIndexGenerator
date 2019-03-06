package com.github.fabianmurariu.esoffline

import java.net.URI

import com.optimaize.langdetect.i18n.LdLocale
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http._
import monix.execution.Scheduler
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

class offlineTest extends FlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()


  "Offline Index" should "start an ES instance and configure the language ingest pipeline" in {
    import com.sksamuel.elastic4s.http.ElasticDsl._
    import offline._
    import spark.implicits._

    val a1: Dataset[WebDocument] = loadWETFiles("data/*.warc.wet.gz")
    val counts = a1.sample(0.1).indexPartitionHttp[Int, String => Option[LdLocale]](
      batchSize = 10,
      dest = new URI("./repo"), indices = Seq("docs"),
      init = EsLang.createPipeline,
      partitionFn = EsLang.langDetect) {
      (client: ElasticClient, detectLang: String => Option[LdLocale], wds: Seq[WebDocument]) =>
        client.execute(
          bulk(
            wds.map {
              case WebDocument(_, date, length, mime, url, _, _, text) =>
                val detectedLang = detectLang(text).flatMap(i => Option(i.getLanguage))
                val textFieldName = detectedLang match {
                  case Some("ar") => "field_ar"
                  case Some("bn") => "field_bn"
                  case Some("bg") => "field_bg"
                  case Some("ca") => "field_ca"
                  case Some("cs") => "field_cz"
                  case Some("en") => "field_en"
                  case Some("de") => "field_de"
                  case Some("fr") => "field_fr"
                  case Some("ru") => "field_ru"
                  case _ => "text"
                }
                indexInto("docs" / "doc")
                  .fields(
                    textFieldName -> text,
                    "lang" -> detectedLang.getOrElse("UNKNOWN"),
                    "url" -> url,
                    "mime" -> mime,
                    "length" -> length,
                    "date" -> date
                  )
                  .routing("<CONSTANT>")
                  .refresh(RefreshPolicy.IMMEDIATE)
            }
          )
        ).await
        1
    }.count

    assert(counts > 0)

    implicit val sched = Scheduler.io()
    implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    EsLang.renameIndicesAndSnapshot("repo").runSyncUnsafe()

  }

}

