package com.github.fabianmurariu.esoffline

import com.github.fabianmurariu.esoffline.offline.{FailedToCreateIndex, FailedToCreateIndexTemplate}
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.i18n.LdLocale
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.text.CommonTextObjectFactories
import com.sksamuel.elastic4s.http.index.CreateIndexTemplateResponse
import com.sksamuel.elastic4s.http.snapshots.CreateRepositoryResponse
import com.sksamuel.elastic4s.http.{ElasticClient, RequestFailure, RequestSuccess, Response}
import monix.eval.Task
import org.apache.log4j.Logger

object EsLang {

  @transient lazy val LOG: Logger = Logger.getLogger(this.getClass)

  val supportedLang: Map[String, String] = Seq("arabic" -> "ar", "basque" -> "eu",
    "bengali" -> "bn", "brazilian" -> "pt", "bulgarian" -> "bg", "catalan" -> "ca",
    "czech" -> "cs", "danish" -> "da", "dutch" -> "nl",
    "english" -> "en", "finnish" -> "fi", "french" -> "fr",
    "german" -> "de", "greek" -> "el", "hindi" -> "hi", "hungarian" -> "hu",
    "indonesian" -> "id", "irish" -> "ga", "italian" -> "it", "latvian" -> "lv",
    "lithuanian" -> "lt", "norwegian" -> "no", "persian" -> "fa", "portuguese" -> "pt",
    "romanian" -> "ro", "russian" -> "ru", "spanish" -> "es",
    "swedish" -> "sv", "turkish" -> "tr", "thai" -> "th", "smartcn" -> "zh",
    "smartcn" -> "cn", "kuromoji" -> "ja", "nori" -> "ko").map(_.swap).toMap

  def createPipeline(elasticClient: ElasticClient, conf:OfflineConf): Task[ElasticClient] = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    val createTemplateTask: Task[Response[CreateIndexTemplateResponse]] = Task.deferFuture {
      elasticClient.execute(
        createIndexTemplate("default", "*").create(true).settings(
          Map(
            "number_of_shards" -> s"${conf.partitions}",
            "number_of_replicas" -> "0",
            "refresh_interval" -> "-1")
        ).order(0)
      )
    }

    val createIndexTask: Task[Response[CreateRepositoryResponse]] = Task.deferFuture {
      val langFields = supportedLang.map { case (lang, analyzer) =>
        textField(s"field_$lang").analyzer(analyzer).store(conf.store)
      }.toList
      elasticClient.execute {
        createIndex("docs").mappings(
          mapping("doc").fields(List(
            textField("text").analyzer("standard").store(conf.store),
            textField("lang"),
            textField("url"),
            textField("host"),
            textField("mime"),
            longField("length"),
            dateField("date")) ++ langFields
          )
        )
        createRepository("offline-backup", "fs").settings(Map(
          "location" -> ".",
          "compress" -> "false",
          "max_snapshot_bytes_per_sec" -> "400mb"))
      }
    }

    createTemplateTask.flatMap {
      case _: RequestSuccess[CreateIndexTemplateResponse] => createIndexTask
      case rf: RequestFailure => throw FailedToCreateIndexTemplate(rf.toString)
    }.map {
      case _: RequestSuccess[CreateRepositoryResponse] => elasticClient
      case rf => throw FailedToCreateIndex(rf.toString)
    }
  }

  def langDetect: String => Option[LdLocale] = {
    //load all languages://load all languages:

    val languageProfiles = new LanguageProfileReader().readAllBuiltIn

    //build language detector:
    val languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard).withProfiles(languageProfiles).build

    //create a text object factory
    val textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText

    { text: String =>
      val textObject = textObjectFactory.forText(text)
      val maybeLang = languageDetector.detect(textObject)
      if (maybeLang.isPresent) Some(maybeLang.get())
      else None
    }
  }

}
