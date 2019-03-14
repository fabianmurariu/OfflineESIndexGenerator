package com.github.fabianmurariu.esoffline

import java.net.URI

import com.google.common.net.InternetDomainName
import com.optimaize.langdetect.i18n.LdLocale
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http.ElasticClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import monix.eval.Task

import scala.util.Try

case class WebDocument(origin: String, date: String, length: Long, mime: String, url: String, version: String, recordId: String, text: String)

object WebDocument {
  implicit val offlineIndexable: OfflineIndexable[String => Option[LdLocale], Int, WebDocument] = new OfflineIndexable[String => Option[LdLocale], Int, WebDocument] {
    override def routing(t: WebDocument): String = "<CONSTANT>"

    override def partitionContext: String => Option[LdLocale] = EsLang.langDetect

    override def init(tc: ElasticClient): Task[ElasticClient] = EsLang.createPipeline(tc)

    override def indices: Seq[String] = Seq("docs")

    override def indexBatch(client: ElasticClient, detectLang: String => Option[LdLocale], wds: Seq[WebDocument]): Int = {
      client.execute(
        bulk(
          wds.map {
            case WebDocument(_, date, length, mime, url, _, _, text) =>
              val detectedLang = detectLang(text).flatMap(i => Option(i.getLanguage))
              val lang = detectedLang.getOrElse("UNKNOWN")
              val textFieldName = EsLang.supportedLang.get(lang).map(name => s"field_$name").getOrElse("text")
              val host = Try(URI.create(url).getHost)
              val domain = host.map(h => InternetDomainName.from(h).topPrivateDomain().name())
              indexInto("docs" / "doc")
                .fields(
                  textFieldName -> text,
                  "lang" -> lang,
                  "url" -> url,
                  "host" -> host.getOrElse(""),
                  "domain" -> domain.getOrElse(""),
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
    }
  }
}

case class WARCIndexDoc(languages: String, url: String)
