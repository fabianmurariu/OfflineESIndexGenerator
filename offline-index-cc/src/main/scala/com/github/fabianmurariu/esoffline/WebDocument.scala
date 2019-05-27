package com.github.fabianmurariu.esoffline

import java.net.URI

import com.google.common.net.InternetDomainName
import com.optimaize.langdetect.i18n.LdLocale
import com.sksamuel.elastic4s.http.ElasticClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.RefreshPolicy
import monix.eval.Task

import scala.util.Try

case class WebDocument(origin: String, date: String, length: Long, mime: String, url: String, version: String, recordId: String, text: String, topDomain: Option[String] = None)

object WebDocument {

  def privateTopDomain(url: String): Option[String] = {
    val host = Try(URI.create(url).getHost)
    host.map(h => InternetDomainName.from(h).topPrivateDomain().name()).toOption
  }

  implicit val offlineIndexable: OfflineIndex[String => Option[LdLocale], Int, WebDocument] = new OfflineIndex[String => Option[LdLocale], Int, WebDocument] {
    def routing(t: WebDocument): String = "<CONSTANT>"

    override def partitionContext: String => Option[LdLocale] = LanguageSupport.langDetect

    override def init(shards:OfflineConf)( tc: ElasticClient): Task[ElasticClient] = EsLang.createPipeline(tc, shards)

    override def indices: Seq[String] = Seq("docs")

    override def indexBatch(client: ElasticClient, detectLang: String => Option[LdLocale], wds: Seq[WebDocument]): Int = {
      client.execute(
        bulk(
          wds.map {
            case wt@WebDocument(_, date, length, mime, url, _, _, text, topDomain) =>
              val detectedLang = detectLang(text).flatMap(i => Option(i.getLanguage))
              val lang = detectedLang.getOrElse("UNKNOWN")
              val textFieldName = EsLang.supportedLang.get(lang).map(name => s"field_$name").getOrElse("text")
              val host = Try(URI.create(url).getHost)
              indexInto("docs" / "doc")
                .fields(
                  textFieldName -> text,
                  "lang" -> lang,
                  "url" -> url,
                  "host" -> host.getOrElse(""),
                  "domain" -> topDomain.getOrElse(""),
                  "mime" -> mime,
                  "length" -> length,
                  "date" -> date
                )
                .routing(routing(wt))
                .refresh(RefreshPolicy.WaitFor)
          }
        )
      ).await
      1
    }
  }
}

case class WARCIndexDoc(languages: String, url: String)
